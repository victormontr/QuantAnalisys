# ===================================================================
#   Volatility Targeting no BOVA11 — Pipeline Modular, Completo e Auditado
# ===================================================================

# ---- Utilidades e Config -----------------------------------------------------
`%||%` <- function(x, y) if (is.null(x)) y else x

cfg <- list(
  # Datas e ativo
  data_inicial = as.Date("2011-01-01"),
  data_final   = as.Date("2025-08-31"),
  ativo        = "BOVA11.SA",
  
  # Convenções
  dias_ano   = 252,
  
  # SELIC
  sgs_cod_primary   = 1178,
  sgs_cod_fallback  = 11,
  sgs_block_years   = 10L,
  
  # EWMA
  lambda_ewma_fast  = 0.94,
  lambda_ewma_slow  = 0.97,
  
  # GARCH rolling
  garch_refit_every = 5L,
  garch_parallel    = FALSE,
  garch_solver      = "hybrid",
  
  # Estratégia VT
  sigma_targets_aa = c(0.10, 0.12, 0.15),
  cap_vec          = c(1.0, 1.5),
  sigma_floor_d    = 0.004,
  sigma_ceiling_d  = 0.08,
  smooth_k         = 5,
  max_leverage     = 1.5,
  dd_threshold     = NA_real_,
  dd_scale         = 0.5,
  
  # Custos e fricções
  cost_bps_vec         = c(0, 10, 25),
  extra_slippage_bps   = 0,
  ter_etf_aa           = 0.00,
  finance_cost_on_lev  = TRUE,
  
  # Outliers
  winsorize_ret_probs = NULL,
  
  # Bootstrap e testes
  bootstrap_B  = 500L,
  block_len    = 10L,
  
  # Split OOS
  split_years  = 3L,
  
  # Janelas de robustez
  rolling_win_days = 252*3,
  rolling_step_days= 21,
  
  # Semente
  seed         = 123
)

quiet_require <- function(pkgs) {
  miss <- setdiff(pkgs, rownames(installed.packages()))
  if (length(miss)) suppressWarnings(install.packages(miss, dependencies = TRUE, quiet = TRUE, repos = "https://cran.rstudio.com"))
  suppressPackageStartupMessages(invisible(lapply(pkgs, require, character.only = TRUE)))
}

dir.create("entradas", recursive = TRUE, showWarnings = FALSE)
dir.create("saídas/dados", recursive = TRUE, showWarnings = FALSE)
dir.create("saídas/tabelas", recursive = TRUE, showWarnings = FALSE)
dir.create(file.path("saídas","Gráficos"), recursive = TRUE, showWarnings = FALSE)

quiet_require(c("yfR","dplyr","readr","lubridate","tibble","tidyr","ggplot2","zoo","jsonlite","scales"))
if (!requireNamespace("rugarch", quietly = TRUE)) install.packages("rugarch", dependencies = TRUE, repos = "https://cran.rstudio.com")
suppressPackageStartupMessages(library(rugarch))
if (!requireNamespace("FinTS", quietly = TRUE)) { try(install.packages("FinTS", repos = "https://cran.rstudio.com")) }
if (!requireNamespace("MCS", quietly = TRUE))  { try(install.packages("MCS",  repos = "https://cran.rstudio.com")) }
if (!requireNamespace("tseries", quietly = TRUE)) { try(install.packages("tseries", repos = "https://cran.rstudio.com")) }

# ---- Métricas e helpers financeiros -----------------------------------------
ann_ret <- function(x, dias_ano = cfg$dias_ano){
  x <- as.numeric(x); x <- x[is.finite(x)]
  n <- length(x); if (n == 0) return(NA_real_)
  prod(1 + x)^(dias_ano/n) - 1
}
ann_vol <- function(x, dias_ano = cfg$dias_ano) stats::sd(x, na.rm = TRUE) * sqrt(dias_ano)
sharpe  <- function(x, rf = NULL, dias_ano = cfg$dias_ano){
  y <- if (is.null(rf)) x else x - rf
  m <- mean(y, na.rm = TRUE); s <- stats::sd(y, na.rm = TRUE)
  if (!is.finite(s) || s == 0) NA_real_ else m/s * sqrt(dias_ano)
}
sortino <- function(x, dias_ano = cfg$dias_ano){
  dn <- x[x < 0]; if (!length(dn)) return(Inf)
  m_ann <- ann_ret(x, dias_ano)
  dd <- stats::sd(dn, na.rm = TRUE) * sqrt(dias_ano)
  if (!is.finite(dd) || dd == 0) NA_real_ else m_ann / dd
}
eq_from_simple <- function(r) cumprod(1 + replace(as.numeric(r), !is.finite(r), 0))
mdd    <- function(x){ idx <- eq_from_simple(x); min(idx / cummax(idx) - 1, na.rm = TRUE) }
calmar <- function(x){ cagr <- ann_ret(x); d <- abs(mdd(x)); if (!is.finite(d) || d == 0) Inf else cagr / d }

vol_a_to_d <- function(x, dias_ano = cfg$dias_ano) x / sqrt(dias_ano)

ewma_sigma_vec <- function(x, lambda = cfg$lambda_ewma_fast){
  x <- as.numeric(x); n <- length(x)
  if (n < 10) return(rep(NA_real_, n))
  v_init <- stats::var(head(x, min(250, n)), na.rm = TRUE)
  s2 <- stats::filter(x = (1 - lambda) * x^2, filter = lambda, method = "recursive", init = v_init)
  sqrt(as.numeric(s2))
}

smooth_clip <- function(s, k = cfg$smooth_k, lo = cfg$sigma_floor_d, hi = cfg$sigma_ceiling_d){
  if (is.numeric(k) && k >= 3 && (k %% 2 == 1)) s <- stats::runmed(s, k = k, endrule = "median")
  pmin(pmax(s, lo), hi)
}

apply_risk_controls <- function(w, dd_series, max_lev = cfg$max_leverage, dd_th = cfg$dd_threshold, dd_scale = cfg$dd_scale){
  w <- pmin(w, max_lev)
  if (is.finite(dd_th) && !all(is.na(dd_series))) {
    scale_vec <- ifelse(dd_series <= -abs(dd_th), dd_scale, 1)
    w <- w * scale_vec
  }
  w
}

winsorize <- function(x, probs = cfg$winsorize_ret_probs){
  if (is.null(probs)) return(x)
  q <- stats::quantile(x, probs, na.rm = TRUE)
  pmin(pmax(x, q[1]), q[2])
}

# ---- 1) Dados base (Yahoo + SELIC) ------------------------------------------
baixar_sgs_csv <- function(codigo, ini, fim, block_years = cfg$sgs_block_years){
  .attempt_read <- function(url, tries = 3) {
    loc <- readr::locale(decimal_mark = ",", grouping_mark = ".", date_names = "pt", encoding = "Latin1")
    for (i in seq_len(tries)) {
      res <- try(
        readr::read_csv2(
          url,
          col_types = readr::cols(
            data  = readr::col_date(format = "%d/%m/%Y"),
            valor = readr::col_number()
          ),
          locale = loc, progress = FALSE
        ),
        silent = TRUE
      )
      if (!inherits(res, "try-error") && is.data.frame(res) && nrow(res) > 0) return(res)
      Sys.sleep(1.5 ^ i)
    }
    NULL
  }
  partes <- list(); inicio <- ini
  while (inicio <= fim) {
    fim_parte <- min(inicio + lubridate::years(block_years) - lubridate::days(1), fim)
    url <- sprintf(
      "https://api.bcb.gov.br/dados/serie/bcdata.sgs.%s/dados?formato=csv&dataInicial=%s&dataFinal=%s",
      as.integer(codigo), format(inicio, "%d/%m/%Y"), format(fim_parte, "%d/%m/%Y")
    )
    dados <- .attempt_read(url)
    if (!is.null(dados) && nrow(dados) > 0) {
      names(dados) <- tolower(names(dados))
      if (!all(c("data","valor") %in% names(dados))) stop("CSV SGS inválido.")
      partes[[length(partes) + 1]] <- tibble::tibble(
        data  = as.Date(dados$data),
        valor = as.numeric(dados$valor)
      )
    }
    inicio <- fim_parte + lubridate::days(1)
  }
  if (!length(partes)) return(NULL)
  dplyr::bind_rows(partes) |>
    dplyr::arrange(data) |>
    dplyr::distinct(data, .keep_all = TRUE) |>
    tibble::as_tibble()
}

processar_selic_aa_para_diaria <- function(dados_brutos, codigo_serie){
  if (is.null(dados_brutos) || !nrow(dados_brutos)) return(NULL)
  dados_brutos |>
    dplyr::transmute(
      data    = as.Date(data),
      taxa_aa = as.numeric(valor) / 100
    ) |>
    dplyr::arrange(data) |>
    dplyr::filter(!is.na(taxa_aa)) |>
    dplyr::mutate(
      fonte     = paste0("SGS_", as.integer(codigo_serie)),
      rf_diaria = (1 + taxa_aa)^(1/cfg$dias_ano) - 1
    ) |>
    tibble::as_tibble()
}

preparar_dados_base <- function(ativo = cfg$ativo, data_inicial = cfg$data_inicial, data_final = cfg$data_final){
  precos <- suppressWarnings(suppressMessages(
    yfR::yf_get(
      tickers = ativo,
      first_date = data_inicial,
      last_date  = data_final,
      freq_data = "daily",
      do_complete_data = TRUE,
      cache_folder = tempfile()
    )
  )) |>
    dplyr::transmute(
      data           = ref_date,
      ativo          = ticker,
      preco_ajustado = price_adjusted,
      volume         = volume
    ) |>
    dplyr::arrange(data)
  
  readr::write_rds(precos, "entradas/precos_bova11_yahoo.rds")
  readr::write_csv(precos, "entradas/precos_bova11_yahoo.csv")
  
  selic_diaria <- baixar_sgs_csv(cfg$sgs_cod_primary, data_inicial, data_final) |>
    processar_selic_aa_para_diaria(cfg$sgs_cod_primary)
  if (is.null(selic_diaria))
    selic_diaria <- baixar_sgs_csv(cfg$sgs_cod_fallback, data_inicial, data_final) |>
    processar_selic_aa_para_diaria(cfg$sgs_cod_fallback)
  
  if (!is.null(selic_diaria) && nrow(selic_diaria) > 0) {
    readr::write_rds(selic_diaria, "entradas/selic_diaria_raw.rds")
    readr::write_csv(selic_diaria, "entradas/selic_diaria_raw.csv")
  } else {
    selic_diaria <- tibble::tibble(data = as.Date(character()), rf_diaria = numeric())
  }
  
  dados_ret <- precos |>
    dplyr::arrange(data) |>
    dplyr::mutate(ret_log = log(preco_ajustado / dplyr::lag(preco_ajustado))) |>
    dplyr::filter(!is.na(ret_log))
  
  dados_ret$ret_log <- winsorize(dados_ret$ret_log, cfg$winsorize_ret_probs)
  
  base_final <- dados_ret |>
    dplyr::left_join(dplyr::select(selic_diaria, data, rf_diaria), by = "data") |>
    dplyr::arrange(data) |>
    tidyr::fill(rf_diaria, .direction = "down") |>
    dplyr::mutate(
      idx_preco = preco_ajustado / dplyr::first(preco_ajustado),
      idx_ret   = exp(cumsum(dplyr::coalesce(ret_log, 0))),
      dd        = idx_ret / cummax(idx_ret) - 1
    )
  
  ok1 <- try(readr::write_rds(base_final, "saídas/dados/bova11_diario.rds"), silent = TRUE)
  ok2 <- try(readr::write_csv(base_final, "saídas/dados/bova11_diario.csv"), silent = TRUE)
  if (!inherits(ok1, "try-error") && !inherits(ok2, "try-error") &&
      file.exists("saídas/dados/bova11_diario.rds") && file.exists("saídas/dados/bova11_diario.csv")) {
    cat("OK: base final salva em saídas/dados (bova11_diario.rds, .csv).\n")
  }
  
  data_ini   <- base_final$data[1]
  data_split <- data_ini %m+% years(cfg$split_years)
  i_split    <- which(base_final$data >= data_split)[1]
  if (is.na(i_split) || i_split <= 1) stop("Amostra insuficiente.")
  
  list(base = base_final, i_split = i_split)
}

# ---- 2) Previsões de volatilidade (EWMA + GARCHs) ---------------------------
calcular_previsoes_vol <- function(base_ext, i_split){
  ret_log <- base_ext$ret_log
  dates   <- base_ext$data
  
  ewma_094 <- dplyr::lag(ewma_sigma_vec(ret_log, cfg$lambda_ewma_fast))
  ewma_097 <- dplyr::lag(ewma_sigma_vec(ret_log, cfg$lambda_ewma_slow))
  
  # especificações
  specs <- list(
    sigma_garch_norm = ugarchspec(variance.model = list(model = "sGARCH", garchOrder = c(1,1)),
                                  mean.model     = list(armaOrder = c(0,0), include.mean = FALSE),
                                  distribution.model = "norm"),
    sigma_garch_std  = ugarchspec(variance.model = list(model = "sGARCH", garchOrder = c(1,1)),
                                  mean.model     = list(armaOrder = c(0,0), include.mean = FALSE),
                                  distribution.model = "std"),
    sigma_egarch_norm= ugarchspec(variance.model = list(model = "eGARCH", garchOrder = c(1,1)),
                                  mean.model     = list(armaOrder = c(0,0), include.mean = FALSE),
                                  distribution.model = "norm"),
    sigma_egarch_std = ugarchspec(variance.model = list(model = "eGARCH", garchOrder = c(1,1)),
                                  mean.model     = list(armaOrder = c(0,0), include.mean = FALSE),
                                  distribution.model = "std"),
    sigma_gjr_norm   = ugarchspec(variance.model = list(model = "gjrGARCH", garchOrder = c(1,1)),
                                  mean.model     = list(armaOrder = c(0,0), include.mean = FALSE),
                                  distribution.model = "norm"),
    sigma_gjr_std    = ugarchspec(variance.model = list(model = "gjrGARCH", garchOrder = c(1,1)),
                                  mean.model     = list(armaOrder = c(0,0), include.mean = FALSE),
                                  distribution.model = "std")
  )
  
  n.start <- i_split - 1L
  roll_try <- function(spec){
    try(
      ugarchroll(
        spec, data = ret_log, n.ahead = 1,
        n.start = n.start, refit.every = cfg$garch_refit_every,
        refit.window = "expanding",
        calculate.VaR = FALSE, keep.coef = TRUE,
        solver = cfg$garch_solver, parallel = cfg$garch_parallel
      ),
      silent = TRUE
    )
  }
  extract_sigma <- function(roll_obj){
    df <- try(as.data.frame(roll_obj), silent = TRUE)
    if (inherits(df, "try-error")) return(NULL)
    idx <- suppressWarnings(as.Date(df$Index))
    if (all(is.na(idx))) idx <- suppressWarnings(as.Date(rownames(df)))
    if (all(is.na(idx))) return(NULL)
    tibble::tibble(data = idx, Sigma = as.numeric(df$Sigma))
  }
  
  cat("Calculando previsões rolling GARCH/EGARCH/GJR...\n")
  pb <- utils::txtProgressBar(min = 0, max = length(specs), style = 3)
  k <- 0
  vol_map <- list()
  for (nm in names(specs)) {
    vol_map[[nm]] <- extract_sigma(roll_try(specs[[nm]]))
    k <- k + 1; setTxtProgressBar(pb, k)
  }
  close(pb); cat("\n")
  
  if (all(vapply(vol_map, is.null, logical(1)))) {
    cat("ugarchroll indisponível. Fallback = EWMA global.\n")
    tibble::tibble(
      data           = dates,
      sigma_ewma_094 = ewma_094,
      sigma_ewma_097 = ewma_097
    )
  } else {
    out <- tibble::tibble(data = dates) |>
      dplyr::mutate(sigma_ewma_094 = ewma_094, sigma_ewma_097 = ewma_097)
    for (nm in names(vol_map)) {
      if (!is.null(vol_map[[nm]])) {
        out <- dplyr::left_join(out, dplyr::rename(vol_map[[nm]], !!nm := Sigma), by = "data")
      }
    }
    out
  }
}

# ---- 3) Montagem de sigma por modelo + pesos por grid ------------------------
montar_sigma_models <- function(base_ext, vol_tbl){
  candidates <- intersect(names(vol_tbl),
                          c("sigma_ewma_094","sigma_ewma_097",
                            "sigma_garch_norm","sigma_garch_std",
                            "sigma_egarch_norm","sigma_egarch_std",
                            "sigma_gjr_norm","sigma_gjr_std"))
  long <- tidyr::pivot_longer(dplyr::select(vol_tbl, data, dplyr::all_of(candidates)),
                              -data, names_to = "modelo", values_to = "sigma_d")
  long$sigma_d <- smooth_clip(long$sigma_d, cfg$smooth_k, cfg$sigma_floor_d, cfg$sigma_ceiling_d)
  long
}

gerar_pesos_vt <- function(base_ext, sigma_long){
  set.seed(cfg$seed)
  sigma_targets_d <- vol_a_to_d(cfg$sigma_targets_aa)
  
  grid <- expand.grid(
    modelo = unique(sigma_long$modelo),
    sigma_target_d = sigma_targets_d,
    cap = cfg$cap_vec,
    stringsAsFactors = FALSE, KEEP.OUT.ATTRS = FALSE
  )
  
  mk_legacy_name <- function(modelo, st_d, cap){
    tgt_a <- round(st_d * sqrt(cfg$dias_ano) * 100)
    base <- switch(modelo,
                   sigma_ewma_094 = "wEW094",
                   sigma_ewma_097 = "wEW097",
                   sigma_garch_norm = "w",
                   sigma_garch_std  = "w",
                   sigma_egarch_norm= "w_egarch_norm",
                   sigma_egarch_std = "w_egarch_std",
                   sigma_gjr_norm   = "w_gjr_norm",
                   sigma_gjr_std    = "w_gjr_std",
                   "w"
    )
    sprintf("%s_%02da_cap%s", base, tgt_a, gsub("\\.", "", as.character(cap)))
  }
  
  weights_df <- tibble::tibble(data = base_ext$data)
  param_map  <- tibble::tibble()
  dd_series  <- base_ext$dd
  
  cat("Gerando pesos para a grade de parâmetros...\n")
  pb <- utils::txtProgressBar(min = 0, max = nrow(grid), style = 3)
  for (i in seq_len(nrow(grid))) {
    g <- grid[i, ]
    s <- sigma_long |>
      dplyr::filter(modelo == g$modelo) |>
      dplyr::arrange(data)
    
    stopifnot(nrow(s) == nrow(weights_df))
    
    st_d <- g$sigma_target_d
    cp   <- g$cap
    
    w    <- pmin(cp, st_d / s$sigma_d)
    w[!is.finite(w) | is.na(w)] <- 1
    w    <- apply_risk_controls(w, dd_series, cfg$max_leverage, cfg$dd_threshold, cfg$dd_scale)
    
    nm   <- mk_legacy_name(g$modelo, st_d, cp)
    if (!nm %in% names(weights_df)) {
      weights_df[[nm]] <- w
      param_map <- dplyr::bind_rows(param_map, tibble::tibble(
        serie_peso = nm,
        modelo     = as.character(g$modelo),
        alvo_aa    = st_d * sqrt(cfg$dias_ano),
        cap        = cp
      ))
    }
    setTxtProgressBar(pb, i)
  }
  close(pb); cat("\n")
  
  readr::write_rds(weights_df, "saídas/dados/vt_pesos.rds")
  readr::write_csv(weights_df, "saídas/dados/vt_pesos.csv")
  list(weights_df = weights_df, param_map = param_map)
}

# ---- 4) Backtest único (síncrono/OOS) + custos + financiamento + TER ----------
executar_backtest <- function(base_ext, weights_df, cost_bps_vec = cfg$cost_bps_vec, oos = FALSE){
  R_asset <- exp(base_ext$ret_log) - 1
  rf_d    <- dplyr::coalesce(base_ext$rf_diaria, 0)
  ter_daily <- if (cfg$ter_etf_aa > 0) (1 + cfg$ter_etf_aa)^(1/cfg$dias_ano) - 1 else 0
  
  lag_or_same <- function(x) if (oos) dplyr::lag(x) else x
  
  peso_cols <- setdiff(names(weights_df), "data")
  ret_gross <- tibble::tibble(data = base_ext$data)
  turnover  <- tibble::tibble(data = base_ext$data)
  
  cat(if (oos) "Rodando backtest OOS...\n" else "Rodando backtest síncrono...\n")
  pb <- utils::txtProgressBar(min = 0, max = length(peso_cols), style = 3)
  j <- 0
  for (nm in peso_cols) {
    w_t <- lag_or_same(weights_df[[nm]])
    w_t[is.na(w_t) | !is.finite(w_t)] <- 0
    r   <- w_t * R_asset
    if (isTRUE(cfg$finance_cost_on_lev)) {
      lev_excess <- pmax(w_t - 1, 0)
      r <- r - lev_excess * rf_d
    }
    if (ter_daily != 0) r <- r - ter_daily
    
    r[!is.finite(r) | is.na(r)] <- 0
    ret_gross[[sub("^w", "ret", nm)]] <- r
    
    w_now <- weights_df[[nm]]
    w_now[!is.finite(w_now) | is.na(w_now)] <- 0
    turnover[[sub("^w", "to", nm)]] <- c(0, abs(diff(w_now)))
    
    j <- j + 1; setTxtProgressBar(pb, j)
  }
  close(pb); cat("\n")
  
  ret_cost <- tibble::tibble(data = base_ext$data)
  total_bps <- cost_bps_vec + cfg$extra_slippage_bps
  for (nm in setdiff(names(ret_gross), "data")) {
    to_col <- sub("^ret", "to", nm)
    for (cbps in total_bps) {
      nm_out <- sprintf("%s_c%02d", nm, cbps)
      cost_series <- (cbps / 1e4) * turnover[[to_col]]
      rc <- ret_gross[[nm]] - cost_series
      rc[!is.finite(rc) | is.na(rc)] <- 0
      ret_cost[[nm_out]] <- rc
    }
  }
  
  list(ret_gross = ret_gross, ret_cost = ret_cost, turnover = turnover, R_asset = R_asset)
}

# ---- 5) Métricas, bootstrap e tabelas de sensibilidade -----------------------
make_metrics_tbl <- function(df, cols, rf_series = NULL, tag = ""){
  dplyr::bind_rows(lapply(cols, function(nm){
    x <- df[[nm]]
    tibble::tibble(
      serie    = nm,
      ann_ret  = ann_ret(x),
      ann_vol  = ann_vol(x),
      sharpe   = if (is.null(rf_series)) sharpe(x) else sharpe(x, rf_series),
      sortino  = sortino(x),
      mdd      = mdd(x),
      calmar   = calmar(x),
      n        = sum(is.finite(x)),
      grupo    = tag
    )
  }))
}

mb_indices <- function(n, B, b){
  b <- max(1L, min(as.integer(b), n))
  k <- ceiling(n / b)
  starts <- matrix(sample.int(n - b + 1L, B * k, replace = TRUE), nrow = B)
  idx <- t(apply(starts, 1, function(st){
    id <- unlist(lapply(st, function(s) seq.int(s, length.out = b)))
    id[seq_len(n)]
  }))
  idx
}

bootstrap_metrics <- function(x, B = cfg$bootstrap_B, b = cfg$block_len){
  x <- as.numeric(x); n <- length(x)
  if (n < 5L) return(tibble::tibble(ann_ret_lo=NA_real_, ann_ret_hi=NA_real_,
                                    sharpe_lo=NA_real_,  sharpe_hi=NA_real_,
                                    mdd_lo=NA_real_,     mdd_hi=NA_real_))
  idx_mat <- mb_indices(n, B, b)
  cat("Bootstrap MBB:", B, "amostras...\n")
  pb <- utils::txtProgressBar(min = 0, max = B, style = 3)
  sret <- numeric(B); ssrp <- numeric(B); smdd <- numeric(B)
  for (i in seq_len(B)) {
    idx <- idx_mat[i, ]
    sret[i] <- ann_ret(x[idx])
    ssrp[i] <- sharpe(x[idx])
    smdd[i] <- mdd(x[idx])
    setTxtProgressBar(pb, i)
  }
  close(pb); cat("\n")
  tibble::tibble(
    ann_ret_lo = quantile(sret, 0.025, na.rm = TRUE),
    ann_ret_hi = quantile(sret, 0.975, na.rm = TRUE),
    sharpe_lo  = quantile(ssrp, 0.025, na.rm = TRUE),
    sharpe_hi  = quantile(ssrp, 0.975, na.rm = TRUE),
    mdd_lo     = quantile(smdd, 0.025, na.rm = TRUE),
    mdd_hi     = quantile(smdd, 0.975, na.rm = TRUE)
  )
}

tabelas_metricas <- function(base_ext, ret_sync, ret_sync_cost, ret_oos, ret_oos_cost, ret_bench, param_map){
  rf_vec <- dplyr::coalesce(base_ext$rf_diaria, 0)
  
  vt_sync_nc <- grep("^ret_", names(ret_sync), value = TRUE)
  vt_sync_c  <- grep("^ret_.*_c\\d+$", names(ret_sync_cost), value = TRUE)
  vt_oos_nc  <- grep("^ret_", names(ret_oos), value = TRUE)
  vt_oos_c   <- grep("^ret_.*_c\\d+$", names(ret_oos_cost), value = TRUE)
  bench_cols <- c("ret_bh")
  
  met_bench   <- make_metrics_tbl(ret_bench, bench_cols, rf_vec, "BH")
  met_sync_nc <- make_metrics_tbl(ret_sync,  vt_sync_nc,  rf_vec, "VT_sync_s/ct")
  met_sync_c  <- make_metrics_tbl(ret_sync_cost, vt_sync_c, rf_vec, "VT_sync_c/ct")
  met_oos_nc  <- make_metrics_tbl(ret_oos,   vt_oos_nc,   rf_vec, "VT_OOS_s/ct")
  met_oos_c   <- make_metrics_tbl(ret_oos_cost, vt_oos_c, rf_vec, "VT_OOS_c/ct")
  
  metrics_all <- dplyr::bind_rows(met_bench, met_sync_nc, met_sync_c, met_oos_nc, met_oos_c) |>
    dplyr::arrange(dplyr::desc(sharpe))
  
  readr::write_rds(metrics_all, "saídas/dados/metrics_all.rds")
  readr::write_csv(metrics_all, "saídas/dados/metrics_all.csv")
  
  map_ret_to_peso <- function(nm){
    nm <- as.character(nm)
    out <- nm
    ew_mask <- grepl("^retEW", nm)
    out[ew_mask]  <- sub("^retEW", "wEW", nm[ew_mask])
    out[!ew_mask] <- sub("^ret_",  "w_",  nm[!ew_mask])
    out
  }
  meta <- dplyr::rename(param_map, serie = serie_peso) |>
    dplyr::mutate(serie = sub("^w", "ret", serie))
  
  sens_tbl <- dplyr::left_join(
    dplyr::select(metrics_all, serie, ann_ret, ann_vol, sharpe, sortino, mdd, calmar, n, grupo),
    meta, by = "serie"
  )
  readr::write_csv(sens_tbl, "saídas/tabelas/sensitivity_full.csv")
  
  oos_c <- dplyr::filter(metrics_all, grupo == "VT_OOS_c/ct", grepl("_c\\d+$", serie))
  oos_c <- oos_c |>
    dplyr::mutate(cost_bps = as.integer(sub(".*_c(\\d+)$", "\\1", serie)),
                  serie_peso = map_ret_to_peso(serie)) |>
    dplyr::left_join(dplyr::select(param_map, serie_peso, modelo, alvo_aa, cap), by = "serie_peso")
  
  best_cost_cap <- oos_c |>
    dplyr::filter(!is.na(cap), !is.na(cost_bps)) |>
    dplyr::group_by(modelo, cost_bps, cap) |>
    dplyr::slice_max(order_by = sharpe, n = 1, with_ties = FALSE) |>
    dplyr::ungroup()
  readr::write_csv(best_cost_cap, "saídas/tabelas/sensitivity_best_by_cost_cap.csv")
  
  best_cost <- oos_c |>
    dplyr::filter(!is.na(cost_bps)) |>
    dplyr::group_by(modelo, cost_bps) |>
    dplyr::slice_max(order_by = sharpe, n = 1, with_ties = FALSE) |>
    dplyr::ungroup()
  readr::write_csv(best_cost, "saídas/tabelas/sensitivity_best_by_cost.csv")
  
  metrics_all
}

# ---- 6) Figuras --------------------------------------------------------------
# --- SUBSTITUÍDA: versão consciente de OOS e de custos ---
plot_figuras <- function(base_ext, metrics_all, ret_bench, ret_sync_cost, ret_oos_cost, param_map){
  fig_dir <- file.path("saídas","Gráficos")
  
  pick_best <- function(group_pattern, cost_suffix = "_c10$"){
    cand <- dplyr::filter(metrics_all, grepl(group_pattern, grupo), grepl(cost_suffix, serie))
    if (!nrow(cand)) cand <- dplyr::filter(metrics_all, grepl(group_pattern, grupo))
    if (!nrow(cand)) return(NA_character_)
    cand$serie[which.max(cand$sharpe)]
  }
  
  best_vt <- pick_best("^VT_OOS_c/ct")
  best_ew <- {
    cand <- dplyr::filter(metrics_all, grupo == "VT_OOS_c/ct", grepl("^retEW", serie), grepl("_c10$", serie))
    if (!nrow(cand)) NA_character_ else cand$serie[which.max(cand$sharpe)]
  }
  bh_name <- "ret_bh"
  
  get_series <- function(nm){
    if (nm == "ret_bh") return(ret_bench[[nm]])
    if (!is.na(nm) && nm %in% names(ret_oos_cost)) return(ret_oos_cost[[nm]])
    if (!is.na(nm) && nm %in% names(ret_sync_cost)) return(ret_sync_cost[[nm]])
    stop("Série não encontrada: ", nm)
  }
  
  series_pick <- unique(na.omit(c(bh_name, best_vt, best_ew)))
  if (length(series_pick) < 2) {
    vt_any <- grep("^ret_", union(names(ret_oos_cost), names(ret_sync_cost)), value = TRUE)
    series_pick <- unique(c(bh_name, head(vt_any, 1)))
  }
  
  eq_idx <- function(r) cumprod(1 + replace(as.numeric(r), !is.finite(r) | is.na(r), 0))
  roll_vol_63 <- function(r) sqrt(cfg$dias_ano) * zoo::rollapplyr(as.numeric(r), 63, sd, na.rm = TRUE, fill = NA)
  
  df_eq <- tibble::tibble(data = base_ext$data)
  for (nm in series_pick) df_eq[[nm]] <- eq_idx(get_series(nm))
  df_long <- tidyr::pivot_longer(df_eq, -data, names_to = "serie", values_to = "idx")
  p_eq <- ggplot(df_long, aes(x = data, y = idx, color = serie)) +
    geom_line(linewidth = 0.8) +
    labs(title = "Curvas de capital (base 1)", x = NULL, y = "Índice acumulado") +
    theme_minimal(base_size = 11) + theme(legend.title = element_blank())
  ggsave(file.path(fig_dir, "equity_curves.png"), p_eq, width = 10, height = 5, dpi = 120)
  
  df_vol <- tibble::tibble(data = base_ext$data)
  for (nm in series_pick) df_vol[[nm]] <- roll_vol_63(get_series(nm))
  dfv_long <- tidyr::pivot_longer(df_vol, -data, names_to = "serie", values_to = "vol_a")
  p_vol <- ggplot(dfv_long, aes(x = data, y = vol_a, color = serie)) +
    geom_line(linewidth = 0.7) +
    labs(title = "Volatilidade anualizada rolling (63 dias)", x = NULL, y = "Vol 63d (a.a.)") +
    theme_minimal(base_size = 11) + theme(legend.title = element_blank())
  ggsave(file.path(fig_dir, "vol_rolling_63d.png"), p_vol, width = 10, height = 5, dpi = 120)
  
  if (!is.na(best_vt)) {
    peso_nm <- if (grepl("^retEW", best_vt)) sub("^retEW", "wEW", best_vt) else sub("^ret_", "w_", best_vt)
    weights_df <- readr::read_rds("saídas/dados/vt_pesos.rds")
    if (peso_nm %in% names(weights_df)) {
      m <- regexec("cap(\\d+)", peso_nm); mm <- regmatches(peso_nm, m)[[1]]
      cap_num <- if (length(mm) >= 2) as.numeric(mm[2])/10 else NA_real_
      df_w <- tibble::tibble(data = weights_df$data, w = weights_df[[peso_nm]])
      p_w <- ggplot(df_w, aes(x = data, y = w)) +
        geom_line(linewidth = 0.7) +
        { if (is.finite(cap_num)) geom_hline(yintercept = cap_num, linetype = 2) else NULL } +
        labs(title = paste0("Peso da estratégia: ", peso_nm), x = NULL, y = "w_t") +
        theme_minimal(base_size = 11)
      ggsave(file.path(fig_dir, "weights_best_vt.png"), p_w, width = 10, height = 4.5, dpi = 120)
    }
  }
  
  dd_from_ret <- function(r){ idx <- eq_idx(r); idx / cummax(idx) - 1 }
  df_dd <- tibble::tibble(data = base_ext$data, BH = dd_from_ret(get_series(bh_name)))
  if (!is.na(best_vt)) df_dd$VT <- dd_from_ret(get_series(best_vt))
  dfdd_long <- tidyr::pivot_longer(df_dd, -data, names_to = "serie", values_to = "dd")
  p_dd <- ggplot(dfdd_long, aes(x = data, y = dd, color = serie)) +
    geom_line(linewidth = 0.7) +
    labs(title = "Drawdown ao longo do tempo", x = NULL, y = "DD") +
    scale_y_continuous(labels = scales::percent) +
    theme_minimal(base_size = 11) + theme(legend.title = element_blank())
  ggsave(file.path(fig_dir, "drawdowns.png"), p_dd, width = 10, height = 5, dpi = 120)
}

# ---- 7) Testes estatísticos e janelas de estresse ----------------------------
jk_memmel <- function(x, y){
  x <- as.numeric(x); y <- as.numeric(y)
  ok <- is.finite(x) & is.finite(y); x <- x[ok]; y <- y[ok]
  n <- length(x); if (n < 30) return(tibble::tibble(z = NA_real_, p_two_sided = NA_real_))
  mx <- mean(x); sx <- stats::sd(x); sr1 <- if (sx > 0) mx / sx else NA_real_
  my <- mean(y); sy <- stats::sd(y); sr2 <- if (sy > 0) my / sy else NA_real_
  if (!is.finite(sr1) || !is.finite(sr2)) return(tibble::tibble(z = NA_real_, p_two_sided = NA_real_))
  rho <- stats::cor(x, y)
  v1 <- (1 + 0.5 * sr1^2) / n
  v2 <- (1 + 0.5 * sr2^2) / n
  cov12 <- (rho + 0.5 * sr1 * sr2) / n
  vdiff <- v1 + v2 - 2 * cov12
  if (vdiff <= 0) return(tibble::tibble(z = NA_real_, p_two_sided = NA_real_))
  z <- (sr1 - sr2) / sqrt(vdiff)
  tibble::tibble(z = z, p_two_sided = 2 * (1 - stats::pnorm(abs(z))))
}

tests_e_stress <- function(base_ext, ret_oos_cost, ret_bench){
  sel_oos_c10 <- grep("_c10$", names(ret_oos_cost), value = TRUE)
  jk_rows <- lapply(sel_oos_c10, function(nm){
    res <- jk_memmel(ret_oos_cost[[nm]], ret_bench$ret_bh)
    cbind.data.frame(serie_vs_bh = nm, z = res$z, p_two_sided = res$p_two_sided)
  })
  jk_tbl <- dplyr::bind_rows(jk_rows)
  readr::write_csv(jk_tbl, "saídas/tabelas/sharpe_tests_jk_memmel_vs_bh.csv")
  
  if (requireNamespace("MCS", quietly = TRUE)) {
    Rmat <- as.matrix(data.frame(lapply(sel_oos_c10, function(nm) ret_oos_cost[[nm]])))
    colnames(Rmat) <- sel_oos_c10
    Lmat <- -Rmat
    try({
      mcs <- MCS::MCSprocedure(Loss = Lmat, alpha = 0.10, B = 500, statistic = "Tmax")
      sink("saídas/tabelas/mcs_report.txt"); print(mcs); sink()
    }, silent = TRUE)
  }
  
  rf_vec <- dplyr::coalesce(base_ext$rf_diaria, 0)
  ann_rf <- (1 + rf_vec)^cfg$dias_ano - 1
  regime_selic <- cut(ann_rf,
                      breaks = c(-Inf, 0.06, 0.10, Inf),
                      labels = c("Baixa(<=6%)", "Média(6–10%)", "Alta(>10%)"),
                      right = TRUE)
  dates <- base_ext$data
  mask_covid <- dates >= as.Date("2020-02-20") & dates <= as.Date("2020-09-30")
  
  metrics_by_mask <- function(cols, mask, label){
    dplyr::bind_rows(lapply(cols, function(nm){
      x <- if (nm == "ret_bh") ret_bench[[nm]] else ret_oos_cost[[nm]]
      x <- x[mask]
      tibble::tibble(
        grupo   = label,
        serie   = nm,
        ann_ret = ann_ret(x),
        ann_vol = ann_vol(x),
        sharpe  = sharpe(x - rf_vec[mask]),
        sortino = sortino(x),
        mdd     = mdd(x),
        calmar  = calmar(x),
        n       = sum(is.finite(x))
      )
    }))
  }
  
  sel_cols <- c("ret_bh", sel_oos_c10)
  met_covid   <- metrics_by_mask(sel_cols, mask_covid, "COVID(2020-02-20..2020-09-30)")
  met_regimes <- dplyr::bind_rows(lapply(levels(regime_selic), function(lv){
    metrics_by_mask(sel_cols, regime_selic == lv, paste0("SELIC_", lv))
  }))
  metrics_stress <- dplyr::bind_rows(met_covid, met_regimes) |>
    dplyr::arrange(grupo, dplyr::desc(sharpe))
  readr::write_csv(metrics_stress, "saídas/tabelas/metrics_stress_windows.csv")
  
  list(jk_tbl = jk_tbl, metrics_stress = metrics_stress)
}

# ---- 8) Diagnósticos GARCH em treino ----------------------------------------
diagnosticos_garch <- function(base_ext, i_split){
  tr <- dplyr::slice(base_ext, 1:(i_split-1))
  x  <- tr$ret_log
  spec <- ugarchspec(
    variance.model = list(model = "sGARCH", garchOrder = c(1,1)),
    mean.model     = list(armaOrder = c(0,0), include.mean = FALSE),
    distribution.model = "std"
  )
  fit <- try(ugarchfit(spec, data = x, solver = cfg$garch_solver, solver.control = list(trace = 0)), silent = TRUE)
  if (inherits(fit, "try-error")) return(invisible(NULL))
  
  res  <- residuals(fit, standardize = TRUE)
  lb <- stats::Box.test(res, lag = 20, type = "Ljung-Box")
  arch_lm <- try(if (requireNamespace("FinTS", quietly = TRUE)) FinTS::ArchTest(as.numeric(res), lags = 12) else NULL, silent = TRUE)
  png("saídas/Gráficos/garch_stdres_qqplot.png", width = 900, height = 600)
  qqnorm(res); qqline(res, col = 2); dev.off()
  
  sink("saídas/tabelas/garch_diagnostics.txt")
  cat("== GARCH(1,1) com t-Student — Treino ==\n")
  show(fit)
  cat("\n-- Ljung-Box (lag=20) sobre resíduos padronizados --\n"); print(lb)
  if (!inherits(arch_lm, "try-error") && !is.null(arch_lm)) { cat("\n-- ARCH LM (lags=12) --\n"); print(arch_lm) }
  sink()
}

# ---- 9) Robustez temporal (subperíodos e janelas rolantes) -------------------
robustez_temporal <- function(base_ext, ret_oos_cost, ret_bench){
  dates <- base_ext$data
  n <- length(dates)
  
  k <- 3L
  brks <- unique(c(1, floor(seq(1, n, length.out = k+1))))
  sub_rows <- list()
  for (i in seq_len(k)) {
    a <- brks[i]; b <- if (i < length(brks)) brks[i+1]-1 else n
    mask <- seq_len(n) >= a & seq_len(n) <= b
    cols <- c("ret_bh", grep("_c10$", names(ret_oos_cost), value = TRUE))
    sub_rows[[i]] <- dplyr::bind_rows(lapply(cols, function(nm){
      x <- if (nm == "ret_bh") ret_bench[[nm]] else ret_oos_cost[[nm]]
      x <- x[mask]
      tibble::tibble(
        janela = paste0("SUB_", i),
        serie  = nm,
        ann_ret = ann_ret(x),
        ann_vol = ann_vol(x),
        sharpe  = sharpe(x - dplyr::coalesce(base_ext$rf_diaria,0)[mask]),
        mdd     = mdd(x),
        n       = sum(is.finite(x))
      )
    }))
  }
  sub_tbl <- dplyr::bind_rows(sub_rows)
  readr::write_csv(sub_tbl, "saídas/tabelas/metrics_subperiods.csv")
  
  win <- cfg$rolling_win_days; step <- cfg$rolling_step_days
  start_idx <- seq(1, n - win + 1, by = step)
  roll_rows <- list()
  cols <- c("ret_bh", grep("_c10$", names(ret_oos_cost), value = TRUE))
  
  cat("Calculando métricas em janelas rolantes...\n")
  pb <- utils::txtProgressBar(min = 0, max = length(start_idx), style = 3)
  ctn <- 0
  for (s in start_idx) {
    e <- s + win - 1
    mask <- seq_len(n) >= s & seq_len(n) <= e
    lab <- paste0("ROLL_", dates[s], "_", dates[e])
    roll_rows[[length(roll_rows)+1]] <- dplyr::bind_rows(lapply(cols, function(nm){
      x <- if (nm == "ret_bh") ret_bench[[nm]] else ret_oos_cost[[nm]]
      x <- x[mask]
      tibble::tibble(
        janela = lab,
        serie  = nm,
        ann_ret = ann_ret(x),
        ann_vol = ann_vol(x),
        sharpe  = sharpe(x - dplyr::coalesce(base_ext$rf_diaria,0)[mask]),
        mdd     = mdd(x),
        n       = sum(is.finite(x))
      )
    }))
    ctn <- ctn + 1; setTxtProgressBar(pb, ctn)
  }
  close(pb); cat("\n")
  
  roll_tbl <- dplyr::bind_rows(roll_rows)
  readr::write_csv(roll_tbl, "saídas/tabelas/metrics_rolling_windows.csv")
}

# ---- 10) Seleção final do modelo e winner_strategy ---------------------------
selecionar_vencedor <- function(metrics_all, jk_tbl){
  cand <- metrics_all |>
    dplyr::filter(grupo == "VT_OOS_c/ct", grepl("_c10$", serie))
  if (nrow(cand)) {
    best_row <- cand[which.max(cand$sharpe), ]
    best_nm  <- best_row$serie
    best_jk  <- dplyr::filter(jk_tbl, serie_vs_bh == best_nm)
    winner <- dplyr::mutate(best_row,
                            z_jk = best_jk$z %||% NA_real_,
                            pval_jk = best_jk$p_two_sided %||% NA_real_)
    readr::write_csv(winner, "saídas/tabelas/winner_strategy.csv")
  }
}

# ---- 11) Assumptions/documentação -------------------------------------------
escrever_assumptions <- function(){
  txt <- c(
    "## Premissas e Políticas de Dados",
    "- Preços ajustados de BOVA11 (Yahoo via yfR). Faltas de pregão mantidas; sem forward-fill de preço.",
    "- SELIC diária derivada de séries anuais do SGS (1178; fallback 11) convertida por (1+aa)^(1/252)-1 e preenchida por forward-fill em dias sem observação.",
    "",
    "## Estratégia e Hiperparâmetros",
    sprintf("- EWMA λ_fast=%.3f, λ_slow=%.3f; refit GARCH a cada %d dias úteis.",
            cfg$lambda_ewma_fast, cfg$lambda_ewma_slow, cfg$garch_refit_every),
    sprintf("- Floor/Ceiling diário de volatilidade: [%.3f, %.3f]. Suavização runmed k=%d.",
            cfg$sigma_floor_d, cfg$sigma_ceiling_d, cfg$smooth_k),
    sprintf("- Alvos de vol (a.a.): %s; caps: %s; alavancagem máxima dura: %.2f.",
            paste0(sprintf('%.0f%%', 100*cfg$sigma_targets_aa), collapse=', '),
            paste0(cfg$cap_vec, collapse=', '), cfg$max_leverage),
    if (is.null(cfg$winsorize_ret_probs)) "- Winsorização: desativada." else
      sprintf("- Winsorização de retornos log: quantis %s.", paste(cfg$winsorize_ret_probs, collapse=", ")),
    "",
    "## Custos e Fricções",
    sprintf("- Custos de transação (round-trip): %s bps aplicados sobre |Δw_t|.",
            paste(cfg$cost_bps_vec + cfg$extra_slippage_bps, collapse=', ')),
    if (cfg$finance_cost_on_lev) "- Custo de financiamento: subtraído sobre (w_t-1)+ usando SELIC diária." else "- Custo de financiamento: DESLIGADO.",
    sprintf("- TER do ETF modelado adicionalmente: %.2f%% a.a. (0 se assumido já embutido nos preços).", 100*cfg$ter_etf_aa),
    "",
    "## Testes e Robustez",
    "- Métricas anuais: retorno, vol, Sharpe, Sortino, MDD, Calmar.",
    sprintf("- IC por bootstrap MBB: B=%d, bloco=%d.", cfg$bootstrap_B, cfg$block_len),
    "- Teste de Sharpe JK–Memmel vs. Buy-and-Hold.",
    "- Controle de data snooping: MCS (se pacote disponível).",
    "- Robustez por subperíodos não sobrepostos e janelas rolantes.",
    "- Diagnósticos GARCH: Ljung–Box, ARCH LM e QQ-plot de resíduos padronizados.",
    "",
    "## Restrições Práticas",
    "- Sem vendas a descoberto; rebalanceamento diário; sem restrições explícitas de liquidez além de custos.",
    "- Slippage adicional opcional via extra_slippage_bps.",
    "",
    "## Reprodutibilidade",
    "- Sessão e configuração salvas em 'saídas/tabelas/session_info.txt' e 'saídas/dados/config_vt.json'."
  )
  writeLines(txt, "saídas/tabelas/assumptions.md")
}

# ---- 12) Orquestração (main) -------------------------------------------------
main <- function(){
  set.seed(cfg$seed)
  
  d <- preparar_dados_base()
  base_ext <- d$base
  i_split  <- d$i_split
  
  vol_tbl <- calcular_previsoes_vol(base_ext, i_split)
  readr::write_rds(vol_tbl, "saídas/dados/vol_forecasts_daily.rds")
  readr::write_csv(vol_tbl, "saídas/dados/vol_forecasts_daily.csv")
  
  sigma_long <- montar_sigma_models(base_ext, vol_tbl)
  wp <- gerar_pesos_vt(base_ext, sigma_long)
  weights_df <- wp$weights_df
  param_map  <- wp$param_map
  
  bt_sync <- executar_backtest(base_ext, weights_df, cfg$cost_bps_vec, oos = FALSE)
  bt_oos  <- executar_backtest(base_ext, weights_df, cfg$cost_bps_vec, oos = TRUE)
  
  ret_bench <- tibble::tibble(data = base_ext$data, ret_bh = bt_sync$R_asset)
  readr::write_rds(ret_bench, "saídas/dados/bench_retornos.rds")
  readr::write_csv(ret_bench, "saídas/dados/bench_retornos.csv")
  
  readr::write_rds(bt_sync$ret_gross, "saídas/dados/vt_retornos_sem_custos.rds")
  readr::write_csv(bt_sync$ret_gross, "saídas/dados/vt_retornos_sem_custos.csv")
  readr::write_rds(bt_sync$ret_cost,  "saídas/dados/vt_retornos_com_custos.rds")
  readr::write_csv(bt_sync$ret_cost,  "saídas/dados/vt_retornos_com_custos.csv")
  readr::write_rds(bt_sync$turnover,  "saídas/dados/vt_turnover.rds")
  readr::write_csv(bt_sync$turnover,  "saídas/dados/vt_turnover.csv")
  
  readr::write_rds(bt_oos$ret_gross, "saídas/dados/vt_retornos_oos_sem_custos.rds")
  readr::write_csv(bt_oos$ret_gross, "saídas/dados/vt_retornos_oos_sem_custos.csv")
  readr::write_rds(bt_oos$ret_cost,  "saídas/dados/vt_retornos_oos_com_custos.rds")
  readr::write_csv(bt_oos$ret_cost,  "saídas/dados/vt_retornos_oos_com_custos.csv")
  
  metrics_all <- tabelas_metricas(
    base_ext,
    bt_sync$ret_gross, bt_sync$ret_cost,
    bt_oos$ret_gross,  bt_oos$ret_cost,
    ret_bench, param_map
  )
  
  sel_cols <- c("ret_bh", grep("_c(00|10|25)$", names(bt_oos$ret_cost), value = TRUE))
  get_series <- function(nm){
    if (nm == "ret_bh") return(ret_bench[[nm]])
    if (nm %in% names(bt_oos$ret_cost))  return(bt_oos$ret_cost[[nm]])
    if (nm %in% names(bt_sync$ret_cost)) return(bt_sync$ret_cost[[nm]])
    stop("Série não encontrada: ", nm)
  }
  ci_rows <- lapply(sel_cols, function(nm) cbind.data.frame(serie = nm, bootstrap_metrics(get_series(nm))))
  ci_tbl  <- dplyr::bind_rows(ci_rows)
  readr::write_csv(ci_tbl, "saídas/tabelas/ci_bootstrap_metrics.csv")
  
  tst <- tests_e_stress(base_ext, bt_oos$ret_cost, ret_bench)
  selecionar_vencedor(metrics_all, tst$jk_tbl)
  
  plot_figuras(base_ext, metrics_all, ret_bench, bt_sync$ret_cost, bt_oos$ret_cost, param_map)
  
  diagnosticos_garch(base_ext, i_split)
  robustez_temporal(base_ext, bt_oos$ret_cost, ret_bench)
  
  escrever_assumptions()
  
  try(write(jsonlite::toJSON(cfg, pretty = TRUE, auto_unbox = TRUE), "saídas/dados/config_vt.json"))
  sess <- capture.output(utils::sessionInfo())
  writeLines(sess, "saídas/tabelas/session_info.txt")
  
  cat("Pipeline concluído.\n")
}

# ---- Executa ---------------------------------------------------------------
main()