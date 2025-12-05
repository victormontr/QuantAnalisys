# ===================================================================
#   Pipeline de Extração e Preparação de Dados Financeiros
# ===================================================================

# ---- Pacotes ----
pacotes <- c("yfR","dplyr","readr","lubridate","tibble","tidyr")
faltam  <- setdiff(pacotes, rownames(installed.packages()))
if (length(faltam)) suppressWarnings(
  install.packages(faltam, dependencies = TRUE, quiet = TRUE, repos = "https://cran.rstudio.com")
)
suppressPackageStartupMessages(invisible(lapply(pacotes, require, character.only = TRUE)))

# ---- Pastas ----
dir.create("entradas", recursive = TRUE, showWarnings = FALSE)
dir.create("saídas/dados", recursive = TRUE, showWarnings = FALSE)

# ---- Parâmetros ----
data_inicial <- as.Date("2011-01-01")
data_final   <- as.Date("2025-08-31")
ativo        <- "BOVA11.SA"

# ---- Preços (Yahoo via yfR) -> ENTRADAS ----
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

# ---- SELIC (SGS/BCB via CSV; blocos ≤10 anos; fallback série 11) -> ENTRADAS ----
baixar_sgs_csv <- function(codigo, ini, fim) {
  stopifnot(inherits(ini, "Date"), inherits(fim, "Date"))
  if (ini > fim) { tmp <- ini; ini <- fim; fim <- tmp }
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
    fim_parte <- min(inicio + lubridate::years(10) - lubridate::days(1), fim)
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

processar_dados_selic <- function(dados_brutos, codigo_serie) {
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
      rf_diaria = (1 + taxa_aa)^(1/252) - 1
    ) |>
    tibble::as_tibble()
}

selic_diaria <- baixar_sgs_csv(1178, data_inicial, data_final) |> processar_dados_selic(1178)
if (is.null(selic_diaria)) selic_diaria <- baixar_sgs_csv(11, data_inicial, data_final) |> processar_dados_selic(11)

if (!is.null(selic_diaria) && nrow(selic_diaria) > 0) {
  readr::write_rds(selic_diaria, "entradas/selic_diaria_raw.rds")
  readr::write_csv(selic_diaria, "entradas/selic_diaria_raw.csv")
} else {
  selic_diaria <- tibble::tibble(data = as.Date(character()), rf_diaria = numeric())
}

# ---- Pré-processamento -> SAÍDAS ----
dados_ret <- precos |>
  dplyr::arrange(data) |>
  dplyr::mutate(ret_bova11 = log(preco_ajustado / dplyr::lag(preco_ajustado))) |>
  dplyr::filter(!is.na(ret_bova11))

selic_join <- selic_diaria |> dplyr::select(dplyr::any_of(c("data","rf_diaria")))
if (!"rf_diaria" %in% names(selic_join)) selic_join$rf_diaria <- NA_real_

base_final <- dados_ret |>
  dplyr::left_join(selic_join, by = "data") |>
  dplyr::arrange(data) |>
  tidyr::fill(rf_diaria, .direction = "down") |>
  dplyr::select(data, ativo, preco_ajustado, volume, ret_bova11, rf_diaria)

ok1 <- try(readr::write_rds(base_final, "saídas/dados/bova11_diario.rds"), silent = TRUE)
ok2 <- try(readr::write_csv(base_final, "saídas/dados/bova11_diario.csv"), silent = TRUE)
if (!inherits(ok1, "try-error") && !inherits(ok2, "try-error") &&
    file.exists("saídas/dados/bova11_diario.rds") && file.exists("saídas/dados/bova11_diario.csv")) {
  cat("OK: base final salva em saídas/dados (bova11_diario.rds, bova11_diario.csv).\n")
}

# ---- Derivados rápidos -> SAÍDAS ----
base_ext <- base_final |>
  tidyr::fill(rf_diaria, .direction = "downup") |>
  dplyr::mutate(
    ret_excesso = ret_bova11 - rf_diaria,
    rf_acum     = cumprod(1 + dplyr::coalesce(rf_diaria, 0)) - 1,
    idx_preco   = preco_ajustado / dplyr::first(preco_ajustado),
    idx_ret     = exp(cumsum(dplyr::coalesce(ret_bova11, 0))),
    dd          = idx_ret / cummax(idx_ret) - 1
  )
readr::write_rds(base_ext, "saídas/dados/bova11_diario_ext.rds")
readr::write_csv(base_ext, "saídas/dados/bova11_diario_ext.csv")
cat(sprintf("Resumo: n=%d, MDD=%.2f%%\n", nrow(base_ext), 100 * min(base_ext$dd, na.rm = TRUE)))

# ---- Split amostral (3 anos) ----
stopifnot(!is.unsorted(base_ext$data), !any(duplicated(base_ext$data)))
stopifnot(all(is.finite(base_ext$ret_bova11)))
stopifnot(all(base_ext$rf_diaria > -0.5 & base_ext$rf_diaria < 0.5, na.rm = TRUE))

data_ini   <- base_ext$data[1]
data_split <- data_ini %m+% years(3)
i_split    <- which(base_ext$data >= data_split)[1]
if (is.na(i_split) || i_split <= 1) stop("Amostra insuficiente.")

train <- dplyr::slice(base_ext, 1:(i_split - 1))
test  <- dplyr::slice(base_ext, i_split:dplyr::n())
forecast_origins <- tibble::tibble(t0 = test$data)

readr::write_rds(train, "saídas/dados/train_3y.rds")
readr::write_rds(test,  "saídas/dados/test_after_3y.rds")
readr::write_rds(forecast_origins, "saídas/dados/forecast_origins.rds")
readr::write_csv(train, "saídas/dados/train_3y.csv")
readr::write_csv(test,  "saídas/dados/test_after_3y.csv")
readr::write_csv(forecast_origins, "saídas/dados/forecast_origins.csv")
cat(sprintf("Split OK: train=%d, test=%d, início teste=%s\n", nrow(train), nrow(test), as.character(test$data[1])))

# ---- EWMA vectorizado (rápido) ----
.ewma_sigma_series <- function(x, lambda = 0.94) {
  x <- as.numeric(x); n <- length(x)
  if (n < 10) return(rep(NA_real_, n))
  v <- stats::var(head(x, min(250, n)), na.rm = TRUE)
  s2 <- numeric(n)
  for (i in seq_len(n)) {
    v <- lambda * v + (1 - lambda) * x[i]^2
    s2[i] <- v
  }
  sqrt(s2)
}
ewma_094 <- dplyr::lag(.ewma_sigma_series(base_ext$ret_bova11, 0.94))
ewma_097 <- dplyr::lag(.ewma_sigma_series(base_ext$ret_bova11, 0.97))

# ---- GARCH(1,1) acelerado com ugarchroll (refit a cada k dias) -> SAÍDAS ----
# Configuração de velocidade
cfg_garch <- list(refit_every = 5L, parallel = TRUE)  # ajuste refit_every para trade-off precisão/tempo

# Dependências
if (!requireNamespace("rugarch", quietly = TRUE)) {
  install.packages("rugarch", dependencies = TRUE, repos = "https://cran.rstudio.com")
}
suppressPackageStartupMessages(library(rugarch))

.garch_spec <- function(dist = c("norm","std")) {
  dist <- match.arg(dist)
  ugarchspec(
    variance.model = list(model = "sGARCH", garchOrder = c(1, 1)),
    mean.model     = list(armaOrder = c(0, 0), include.mean = FALSE),
    distribution.model = dist
  )
}
spec_norm <- .garch_spec("norm")
spec_std  <- .garch_spec("std")

ret_all <- base_ext$ret_bova11
dates   <- base_ext$data
n.start <- i_split - 1L

.roll_try <- function(spec) {
  try(
    ugarchroll(
      spec, data = ret_all, n.ahead = 1,
      n.start = n.start,
      refit.every = cfg_garch$refit_every,
      refit.window = "expanding",
      calculate.VaR = FALSE, keep.coef = TRUE,
      solver = "hybrid",
      parallel = cfg_garch$parallel
    ),
    silent = TRUE
  )
}

extract_sigma <- function(roll_obj) {
  df <- try(as.data.frame(roll_obj), silent = TRUE)
  if (inherits(df, "try-error")) return(NULL)
  # Tenta coluna de índice; se não houver, usa rownames
  idx <- suppressWarnings(as.Date(df$Index))
  if (all(is.na(idx))) idx <- suppressWarnings(as.Date(rownames(df)))
  if (all(is.na(idx))) return(NULL)
  tibble::tibble(data = idx, Sigma = as.numeric(df$Sigma))
}

roll_n <- length(dates) - n.start
vol_std <- vol_norm <- NULL

roll_std <- .roll_try(spec_std)
if (!inherits(roll_std, "try-error")) vol_std  <- extract_sigma(roll_std)

roll_norm <- .roll_try(spec_norm)
if (!inherits(roll_norm, "try-error")) vol_norm <- extract_sigma(roll_norm)

if (is.null(vol_std) && is.null(vol_norm)) {
  # Fallback lento (loop diário) — mantém lógica original
  cat("ugarchroll indisponível. Usando fallback lento.\n")
  .ewma_last_sigma <- function(x, lambda = 0.94) {
    x <- x[is.finite(x)]; if (length(x) < 10) return(NA_real_)
    s2 <- stats::var(head(x, 250), na.rm = TRUE)
    for (i in seq_along(x)) s2 <- lambda * s2 + (1 - lambda) * x[i]^2
    sqrt(s2)
  }
  origins <- dates[(n.start + 1L):length(dates)]
  out <- vector("list", length(origins))
  for (k in seq_along(origins)) {
    t0  <- origins[k]; idx  <- which(dates < t0)
    if (length(idx) < 250) {
      out[[k]] <- tibble::tibble(data = t0, sigma_garch_norm = NA_real_, sigma_garch_std = NA_real_)
      next
    }
    x_tr <- ret_all[idx]
    fit_t <- try(ugarchfit(spec_std,  data = x_tr, solver = "hybrid", solver.control = list(trace = 0)), silent = TRUE)
    f_t  <- try(if (!inherits(fit_t, "try-error")) ugarchforecast(fit_t, n.ahead = 1) else NULL, silent = TRUE)
    s_t  <- if (!inherits(f_t, "try-error") && !is.null(f_t)) as.numeric(sigma(f_t))[1] else NA_real_
    fit_n <- try(ugarchfit(spec_norm, data = x_tr, solver = "hybrid", solver.control = list(trace = 0)), silent = TRUE)
    f_n   <- try(if (!inherits(fit_n, "try-error")) ugarchforecast(fit_n, n.ahead = 1) else NULL, silent = TRUE)
    s_n   <- if (!inherits(f_n, "try-error") && !is.null(f_n)) as.numeric(sigma(f_n))[1] else NA_real_
    out[[k]] <- tibble::tibble(data = t0, sigma_garch_norm = s_n, sigma_garch_std = s_t)
    if (k %% 250 == 0) cat(sprintf("Rolling %d/%d...\n", k, length(origins)))
  }
  vol_garch <- dplyr::bind_rows(out)
} else {
  # Junta saídas do ugarchroll por data
  vol_garch <- tibble::tibble(data = dates[(n.start + 1L):length(dates)])
  if (!is.null(vol_std))  vol_garch <- dplyr::left_join(vol_garch, dplyr::rename(vol_std,  sigma_garch_std  = Sigma), by = "data")
  if (!is.null(vol_norm)) vol_garch <- dplyr::left_join(vol_garch, dplyr::rename(vol_norm, sigma_garch_norm = Sigma), by = "data")
}

# Monta tabela final de previsões diárias
vol_forecasts <- vol_garch |>
  dplyr::arrange(data) |>
  dplyr::left_join(tibble::tibble(data = dates, sigma_ewma_094 = ewma_094, sigma_ewma_097 = ewma_097), by = "data")

# Junta previsões à base estendida
base_with_sigmas <- base_ext |>
  dplyr::left_join(vol_forecasts, by = "data")

# Persistência -> SAÍDAS
readr::write_rds(vol_forecasts, "saídas/dados/vol_forecasts_daily.rds")
readr::write_csv(vol_forecasts, "saídas/dados/vol_forecasts_daily.csv")
readr::write_rds(base_with_sigmas, "saídas/dados/bova11_diario_with_sigmas.rds")
readr::write_csv(base_with_sigmas, "saídas/dados/bova11_diario_with_sigmas.csv")
cat("Volatilidades previstas salvas em saídas/dados/vol_forecasts_daily.{rds,csv}.\n")

# ---- Sigma_hat: suavização + limites; pesos VT; backtest básico -> SAÍDAS ----

# Defaults de configuração caso ainda não existam
if (!exists("cfg")) cfg <- list()
if (is.null(cfg$sigma_targets))  cfg$sigma_targets  <- c(0.10, 0.12, 0.15)  # anual
if (is.null(cfg$cap))            cfg$cap            <- c(1.0, 1.5)
if (is.null(cfg$lambda_ewma))    cfg$lambda_ewma    <- 0.94
if (is.null(cfg$sigma_floor_d))  cfg$sigma_floor_d  <- 0.004                 # ~6,3% a.a.
if (is.null(cfg$sigma_ceiling_d))cfg$sigma_ceiling_d<- 0.08                  # ~127% a.a.
if (is.null(cfg$smooth_k))       cfg$smooth_k       <- 5                     # impar >=3
if (is.null(cfg$seed))           cfg$seed           <- 123
set.seed(cfg$seed)

vol_a_to_d <- function(x, days = 252) x / sqrt(days)
sigma_targets_d <- vol_a_to_d(cfg$sigma_targets)

# Escolha de EWMA conforme lambda e sigma_hat bruto
ewma_sel <- if (cfg$lambda_ewma >= 0.965) base_with_sigmas$sigma_ewma_097 else base_with_sigmas$sigma_ewma_094
sigma_hat_raw_d <- dplyr::coalesce(base_with_sigmas$sigma_garch_std,
                                   base_with_sigmas$sigma_garch_norm,
                                   ewma_sel)

# Suavização por mediana móvel (runmed) e aplicação de floor/ceiling
k <- cfg$smooth_k
sigma_hat_sm_d <- sigma_hat_raw_d
if (is.numeric(k) && k >= 3 && (k %% 2 == 1)) {
  sigma_hat_sm_d <- stats::runmed(sigma_hat_raw_d, k = k, endrule = "median")
}
sigma_hat_sm_d <- pmin(pmax(sigma_hat_sm_d, cfg$sigma_floor_d), cfg$sigma_ceiling_d)

# Persistência das séries de sigma
sigma_tbl <- tibble::tibble(
  data            = base_with_sigmas$data,
  sigma_hat_raw_d = sigma_hat_raw_d,
  sigma_hat_sm_d  = sigma_hat_sm_d
)
readr::write_rds(sigma_tbl, "saídas/dados/sigma_hat_series.rds")
readr::write_csv(sigma_tbl, "saídas/dados/sigma_hat_series.csv")

# Pesos VT para todas as combinações alvo x cap
comb <- expand.grid(sigma_target_d = sigma_targets_d,
                    cap = cfg$cap,
                    KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)

mk_name <- function(st_d, cap) {
  tgt_a <- round(st_d * sqrt(252) * 100)          # alvo em % a.a. no nome
  sprintf("w_%02da_cap%s", tgt_a, gsub("\\.", "", as.character(cap)))
}

weights_df <- tibble::tibble(data = base_with_sigmas$data)
for (i in seq_len(nrow(comb))) {
  st_d <- comb$sigma_target_d[i]
  cp   <- comb$cap[i]
  w    <- pmin(cp, st_d / sigma_hat_sm_d)
  w[!is.finite(w)] <- 1
  nm   <- mk_name(st_d, cp)
  weights_df[[nm]] <- w
}

readr::write_rds(weights_df, "saídas/dados/vt_pesos.rds")
readr::write_csv(weights_df, "saídas/dados/vt_pesos.csv")

# Backtest básico sem custos: retorno = w_t * retorno do ativo
ret_base <- tibble::tibble(
  data        = base_with_sigmas$data,
  ret_bova11  = base_with_sigmas$ret_bova11
)

ret_df <- ret_base
peso_cols <- setdiff(names(weights_df), "data")
for (nm in peso_cols) {
  r <- weights_df[[nm]] * ret_base$ret_bova11
  r[!is.finite(r)] <- 0
  ret_df[[sub("^w_", "ret_", nm)]] <- r
}

readr::write_rds(ret_df, "saídas/dados/vt_retornos_sem_custos.rds")
readr::write_csv(ret_df, "saídas/dados/vt_retornos_sem_custos.csv")

# ---- Custos de transação, Benchmarks e Métricas -> SAÍDAS ----

# Config
if (is.null(cfg$cost_bps)) cfg$cost_bps <- c(0, 10, 25)
if (is.null(cfg$lambda_ewma)) cfg$lambda_ewma <- 0.94
if (is.null(cfg$smooth_k)) cfg$smooth_k <- 5

# Turnover diário por coluna de peso
peso_cols <- setdiff(names(weights_df), "data")
turnover <- tibble::tibble(data = weights_df$data)
for (nm in peso_cols) {
  w <- weights_df[[nm]]
  turnover[[sub("^w_", "to_", nm)]] <- c(0, abs(diff(w)))
}
readr::write_rds(turnover, "saídas/dados/vt_turnover.rds")
readr::write_csv(turnover, "saídas/dados/vt_turnover.csv")

# Retornos com custos (bps * turnover)
ret_cost <- ret_df
for (nm in peso_cols) {
  ret_col <- sub("^w_", "ret_", nm)
  to_col  <- sub("^w_", "to_",  nm)
  for (cbps in cfg$cost_bps) {
    nm_out <- sprintf("%s_c%02d", ret_col, cbps)
    cost_series <- (cbps / 1e4) * turnover[[to_col]]
    r_net <- ret_df[[ret_col]] - cost_series
    r_net[!is.finite(r_net)] <- 0
    ret_cost[[nm_out]] <- r_net
  }
}
readr::write_rds(ret_cost, "saídas/dados/vt_retornos_com_custos.rds")
readr::write_csv(ret_cost, "saídas/dados/vt_retornos_com_custos.csv")

# Benchmarks: Buy-and-Hold e VT-EWMA
# EWMA puro para sigma_hat
ewma_sel <- if (cfg$lambda_ewma >= 0.965) base_with_sigmas$sigma_ewma_097 else base_with_sigmas$sigma_ewma_094
ewma_hat <- ewma_sel
if (is.numeric(cfg$smooth_k) && cfg$smooth_k >= 3 && cfg$smooth_k %% 2 == 1) {
  ewma_hat <- stats::runmed(ewma_hat, k = cfg$smooth_k, endrule = "median")
}
ewma_hat <- pmin(pmax(ewma_hat, 0.004), 0.08)

vol_a_to_d <- function(x, days = 252) x / sqrt(days)
sigma_targets_d <- vol_a_to_d(cfg$sigma_targets %||% c(0.10, 0.12, 0.15))

weights_ew <- tibble::tibble(data = base_with_sigmas$data)
for (st_d in sigma_targets_d) for (cp in (cfg$cap %||% c(1.0, 1.5))) {
  w <- pmin(cp, st_d / ewma_hat)
  w[!is.finite(w)] <- 1
  tgt_a <- round(st_d * sqrt(252) * 100)
  nm <- sprintf("wEW_%02da_cap%s", tgt_a, gsub("\\.", "", as.character(cp)))
  weights_ew[[nm]] <- w
}

# Retornos grossos e com custos para EWMA
ret_bench <- tibble::tibble(data = base_with_sigmas$data, ret_bh = base_with_sigmas$ret_bova11)
to_ew <- tibble::tibble(data = base_with_sigmas$data)
for (nm in setdiff(names(weights_ew), "data")) {
  w <- weights_ew[[nm]]
  r <- w * base_with_sigmas$ret_bova11
  r[!is.finite(r)] <- 0
  ret_bench[[sub("^wEW_", "retEW_", nm)]] <- r
  to_ew[[sub("^wEW_", "toEW_", nm)]] <- c(0, abs(diff(w)))
}

for (nm in setdiff(names(ret_bench), c("data","ret_bh"))) {
  to_col <- sub("^retEW_", "toEW_", nm)
  for (cbps in cfg$cost_bps) {
    nm_out <- sprintf("%s_c%02d", nm, cbps)
    cost_series <- (cbps / 1e4) * to_ew[[to_col]]
    ret_bench[[nm_out]] <- ret_bench[[nm]] - cost_series
  }
}

readr::write_rds(weights_ew, "saídas/dados/vt_ewma_pesos.rds")
readr::write_csv(weights_ew, "saídas/dados/vt_ewma_pesos.csv")
readr::write_rds(to_ew, "saídas/dados/vt_ewma_turnover.rds")
readr::write_csv(to_ew, "saídas/dados/vt_ewma_turnover.csv")
readr::write_rds(ret_bench, "saídas/dados/bench_retornos.rds")
readr::write_csv(ret_bench, "saídas/dados/bench_retornos.csv")

# Métricas: retorno/vol anualizados, Sharpe, Sortino, MDD, Calmar
ann_ret <- function(x) exp(mean(x, na.rm = TRUE) * 252) - 1
ann_vol <- function(x) stats::sd(x, na.rm = TRUE) * sqrt(252)
sharpe  <- function(x, rf = NULL) {
  if (is.null(rf)) return(mean(x, na.rm = TRUE) / stats::sd(x, na.rm = TRUE) * sqrt(252))
  ex <- x - rf
  mean(ex, na.rm = TRUE) / stats::sd(x, na.rm = TRUE) * sqrt(252)
}
sortino <- function(x) {
  dn <- x[x < 0]; if (!length(dn)) return(Inf)
  m  <- mean(x, na.rm = TRUE) * 252
  dd <- stats::sd(dn, na.rm = TRUE) * sqrt(252)
  m / dd
}
mdd    <- function(x) {
  idx <- exp(cumsum(replace(x, !is.finite(x), 0)))
  min(idx / cummax(idx) - 1, na.rm = TRUE)
}
calmar <- function(x) {
  cagr <- ann_ret(x)
  d    <- abs(mdd(x))
  if (d == 0) return(Inf)
  cagr / d
}

# Monta tabela de métricas para um conjunto de colunas
make_metrics <- function(df, cols, rf_series = NULL) {
  out <- lapply(cols, function(nm) {
    x <- df[[nm]]
    tibble::tibble(
      serie    = nm,
      ann_ret  = ann_ret(x),
      ann_vol  = ann_vol(x),
      sharpe   = if (is.null(rf_series)) sharpe(x) else sharpe(x, rf_series),
      sortino  = sortino(x),
      mdd      = mdd(x),
      calmar   = calmar(x),
      n        = sum(is.finite(x))
    )
  })
  dplyr::bind_rows(out)
}

# Seleção de colunas
rf_vec <- dplyr::coalesce(base_ext$rf_diaria, 0)
vt_cols_nc  <- grep("^ret_", names(ret_df), value = TRUE)
vt_cols_c   <- grep("^ret_.*_c\\d+$", names(ret_cost), value = TRUE)
ew_cols_nc  <- grep("^retEW_", names(ret_bench), value = TRUE)
ew_cols_c   <- grep("^retEW_.*_c\\d+$", names(ret_bench), value = TRUE)
bench_cols  <- c("ret_bh")

met_vt_nc <- make_metrics(ret_df,   vt_cols_nc, rf_vec)
met_vt_c  <- make_metrics(ret_cost, vt_cols_c,  rf_vec)
met_ew_nc <- make_metrics(ret_bench, ew_cols_nc, rf_vec)
met_ew_c  <- make_metrics(ret_bench, ew_cols_c,  rf_vec)
met_bench <- make_metrics(ret_bench, bench_cols, rf_vec)

metrics_all <- dplyr::bind_rows(
  dplyr::mutate(met_bench, grupo = "BH"),
  dplyr::mutate(met_vt_nc, grupo = "VT_s/ct"),
  dplyr::mutate(met_vt_c,  grupo = "VT_c/ct"),
  dplyr::mutate(met_ew_nc, grupo = "VT_EWMA_s/ct"),
  dplyr::mutate(met_ew_c,  grupo = "VT_EWMA_c/ct")
) |>
  dplyr::arrange(desc(sharpe))

readr::write_rds(metrics_all, "saídas/dados/metrics_all.rds")
readr::write_csv(metrics_all, "saídas/dados/metrics_all.csv")

# ---- IC por Bootstrap (completo e robusto) -> saídas/tabelas ----
dir.create("saídas/tabelas", recursive = TRUE, showWarnings = FALSE)

# Config padrão
if (!exists("cfg")) cfg <- list()
if (is.null(cfg$bootstrap_B)) cfg$bootstrap_B <- 500L
if (is.null(cfg$block_len))   cfg$block_len   <- 10L

# Métricas mínimas (define se ausentes)
if (!exists("ann_ret")) ann_ret <- function(x) { x <- as.numeric(x); exp(mean(x, na.rm = TRUE) * 252) - 1 }
if (!exists("sharpe"))  sharpe  <- function(x) { x <- as.numeric(x); m <- mean(x, na.rm = TRUE); s <- stats::sd(x, na.rm = TRUE); if (!is.finite(s) || s == 0) NA_real_ else m / s * sqrt(252) }
if (!exists("mdd"))     mdd     <- function(x) { idx <- exp(cumsum(replace(as.numeric(x), !is.finite(x), 0))); min(idx / cummax(idx) - 1, na.rm = TRUE) }

# Índices para moving-block bootstrap
.mb_indices <- function(n, B, b) {
  b <- max(1L, min(as.integer(b), n))
  k <- ceiling(n / b)
  starts <- matrix(sample.int(n - b + 1L, B * k, replace = TRUE), nrow = B)
  idx <- t(apply(starts, 1, function(st) {
    id <- unlist(lapply(st, function(s) seq.int(s, length.out = b)))
    id[seq_len(n)]
  }))
  idx
}

# Bootstrap das métricas
bootstrap_metrics <- function(x, B = cfg$bootstrap_B, b = cfg$block_len) {
  x <- as.numeric(x); n <- length(x)
  if (n < 5L) return(tibble::tibble(ann_ret_lo = NA_real_, ann_ret_hi = NA_real_, sharpe_lo = NA_real_, sharpe_hi = NA_real_, mdd_lo = NA_real_, mdd_hi = NA_real_))
  idx_mat <- .mb_indices(n, B, b)
  sret <- vapply(seq_len(B), function(i) ann_ret(x[idx_mat[i, ]]), numeric(1))
  ssrp <- vapply(seq_len(B), function(i) sharpe(x[idx_mat[i, ]]),   numeric(1))
  smdd <- vapply(seq_len(B), function(i) mdd(x[idx_mat[i, ]]),      numeric(1))
  tibble::tibble(
    ann_ret_lo = quantile(sret, 0.025, na.rm = TRUE),
    ann_ret_hi = quantile(sret, 0.975, na.rm = TRUE),
    sharpe_lo  = quantile(ssrp, 0.025, na.rm = TRUE),
    sharpe_hi  = quantile(ssrp, 0.975, na.rm = TRUE),
    mdd_lo     = quantile(smdd, 0.025, na.rm = TRUE),
    mdd_hi     = quantile(smdd, 0.975, na.rm = TRUE)
  )
}

# Seleção de séries
stopifnot(exists("ret_cost"), exists("ret_bench"), "ret_bh" %in% names(ret_bench))
custos_alvo <- c(0, 10, 25)

vt_cost_cols <- grep("^ret_.*_c\\d+$",   names(ret_cost),  value = TRUE)
ew_cost_cols <- grep("^retEW_.*_c\\d+$", names(ret_bench), value = TRUE)

pick_by_cost <- function(cols, custos) {
  patt <- paste0("_c(", paste(sprintf("%02d", custos), collapse = "|"), ")$")
  grep(patt, cols, value = TRUE)
}

sel_cols <- c(
  "ret_bh",
  pick_by_cost(vt_cost_cols, custos_alvo),
  pick_by_cost(ew_cost_cols, custos_alvo)
)

get_series <- function(nm) {
  if (nm == "ret_bh") return(ret_bench[[nm]])
  if (nm %in% names(ret_cost))  return(ret_cost[[nm]])
  if (nm %in% names(ret_bench)) return(ret_bench[[nm]])
  stop("Série não encontrada: ", nm)
}

ci_rows <- lapply(sel_cols, function(nm) cbind.data.frame(serie = nm, bootstrap_metrics(get_series(nm))))
ci_tbl  <- dplyr::bind_rows(ci_rows)
readr::write_csv(ci_tbl, "saídas/tabelas/ci_bootstrap_metrics.csv")

# ---- Sensibilidade de parâmetros + Margem/Turnover (com correções) -> saídas/tabelas ----
dir.create("saídas/tabelas", recursive = TRUE, showWarnings = FALSE)

# Parsers robustos
.parse_cap <- function(x) ifelse(grepl("cap(\\d+)", x),
                                 as.numeric(sub(".*cap(\\d+).*", "\\1", x))/10, NA_real_)
.parse_tgt <- function(x) ifelse(grepl("(\\d{2})a", x),
                                 as.numeric(sub(".*?(\\d{2})a.*", "\\1", x))/100, NA_real_)
.parse_cost <- function(x) {
  m <- regmatches(x, regexpr("_c\\d+$", x))
  ifelse(nchar(m) > 0, as.integer(sub("_c", "", m)), 0L)
}

parse_ret_name <- function(nm) {
  if (nm == "ret_bh") {
    return(tibble::tibble(serie = nm, modelo = "BH", alvo_aa = NA_real_, cap = 1.0, cost_bps = 0L))
  }
  if (grepl("^retEW_", nm)) {
    tibble::tibble(
      serie = nm, modelo = "VT_EWMA",
      alvo_aa = .parse_tgt(nm), cap = .parse_cap(nm), cost_bps = .parse_cost(nm)
    )
  } else if (grepl("^ret_", nm)) {
    tibble::tibble(
      serie = nm, modelo = "VT",
      alvo_aa = .parse_tgt(nm), cap = .parse_cap(nm), cost_bps = .parse_cost(nm)
    )
  } else {
    tibble::tibble(serie = nm, modelo = NA_character_, alvo_aa = NA_real_, cap = NA_real_, cost_bps = NA_integer_)
  }
}

# Conecta parâmetros às métricas
stopifnot(exists("metrics_all"))
params_tbl <- dplyr::bind_rows(lapply(unique(metrics_all$serie), parse_ret_name)) |>
  dplyr::distinct(serie, .keep_all = TRUE)

sens_tbl <- metrics_all |>
  dplyr::left_join(params_tbl, by = "serie") |>
  dplyr::relocate(modelo, alvo_aa, cap, cost_bps, .after = serie)

readr::write_csv(sens_tbl, "saídas/tabelas/sensitivity_full.csv")

# Melhores por custo e cap
best_cost_cap <- sens_tbl |>
  dplyr::filter(!is.na(cap), !is.na(cost_bps)) |>
  dplyr::group_by(modelo, cost_bps, cap) |>
  dplyr::slice_max(order_by = sharpe, n = 1, with_ties = FALSE) |>
  dplyr::ungroup()
readr::write_csv(best_cost_cap, "saídas/tabelas/sensitivity_best_by_cost_cap.csv")

# Melhores por custo (ignorando cap)
best_cost <- sens_tbl |>
  dplyr::filter(!is.na(cost_bps)) |>
  dplyr::group_by(modelo, cost_bps) |>
  dplyr::slice_max(order_by = sharpe, n = 1, with_ties = FALSE) |>
  dplyr::ungroup()
readr::write_csv(best_cost, "saídas/tabelas/sensitivity_best_by_cost.csv")

# ---- Uso de margem e turnover por estratégia (pesos) ----
stopifnot(exists("weights_df"))
if (!exists("weights_ew")) weights_ew <- tibble::tibble(data = weights_df$data) # garante objeto

summarize_weights <- function(df, kind = c("VT","VT_EWMA")) {
  kind <- match.arg(kind)
  nm_cols <- setdiff(names(df), "data")
  out <- lapply(nm_cols, function(nm) {
    w <- df[[nm]]
    tibble::tibble(
      serie_peso = nm,
      modelo = kind,
      alvo_aa = .parse_tgt(nm),
      cap     = .parse_cap(nm),
      pct_dias_w_gt1 = mean(w > 1, na.rm = TRUE),
      w_max   = max(w, na.rm = TRUE),
      w_med   = mean(w, na.rm = TRUE)
    )
  })
  dplyr::bind_rows(out)
}

margin_vt  <- summarize_weights(weights_df,  "VT")
margin_ew  <- summarize_weights(weights_ew,  "VT_EWMA")
margin_all <- dplyr::bind_rows(margin_vt, margin_ew)

# Turnover: mediana, média e p95
stopifnot(exists("turnover"))
if (!exists("to_ew")) to_ew <- tibble::tibble(data = weights_df$data)
to_ew_df <- to_ew  # evita sobrescrever objeto

turn_summary <- function(turn_df, modelo_prefix = c("to_", "toEW_"), kind = c("VT","VT_EWMA")) {
  modelo_prefix <- match.arg(modelo_prefix)
  kind <- match.arg(kind)
  nm_cols <- grep(paste0("^", modelo_prefix), names(turn_df), value = TRUE)
  dplyr::bind_rows(lapply(nm_cols, function(nm) {
    to <- turn_df[[nm]]
    peso_nm <- if (modelo_prefix == "to_") sub("^to_", "w_", nm) else sub("^toEW_", "wEW_", nm)
    tibble::tibble(
      serie_turnover = nm,
      serie_peso     = peso_nm,
      modelo         = kind,
      alvo_aa        = .parse_tgt(peso_nm),
      cap            = .parse_cap(peso_nm),
      to_median      = stats::median(to, na.rm = TRUE),
      to_p95         = stats::quantile(to, 0.95, na.rm = TRUE),
      to_mean        = mean(to, na.rm = TRUE)
    )
  }))
}

to_vt <- turn_summary(turnover, "to_",   "VT")
to_ew_summary <- turn_summary(to_ew_df, "toEW_", "VT_EWMA")
to_all <- dplyr::bind_rows(to_vt, to_ew_summary)

# Une margem e turnover
margin_turn <- margin_all |>
  dplyr::left_join(to_all, by = c("modelo","alvo_aa","cap","serie_peso"))

readr::write_csv(margin_all, "saídas/tabelas/margin_usage.csv")
readr::write_csv(to_all,     "saídas/tabelas/turnover_summary.csv")
readr::write_csv(margin_turn,"saídas/tabelas/margin_and_turnover.csv")

# ---- Figuras principais -> saídas/Gráficos ----
fig_dir <- file.path("saídas","Gráficos")
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

if (!requireNamespace("ggplot2", quietly = TRUE)) {
  install.packages("ggplot2", dependencies = TRUE, repos = "https://cran.rstudio.com")
}
if (!requireNamespace("zoo", quietly = TRUE)) {
  install.packages("zoo", dependencies = TRUE, repos = "https://cran.rstudio.com")
}
suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(zoo))

eq_idx <- function(r) exp(cumsum(replace(as.numeric(r), !is.finite(r), 0)))
roll_vol_63 <- function(r) sqrt(252) * zoo::rollapplyr(as.numeric(r), 63, sd, na.rm = TRUE, fill = NA)

get_series <- function(nm) {
  if (nm == "ret_bh") return(ret_bench[[nm]])
  if (nm %in% names(ret_cost))  return(ret_cost[[nm]])
  if (nm %in% names(ret_df))    return(ret_df[[nm]])
  if (nm %in% names(ret_bench)) return(ret_bench[[nm]])
  stop("Série não encontrada: ", nm)
}
weight_name_from_ret <- function(nm) {
  if (grepl("^retEW_", nm)) return(sub("^retEW_", "wEW_", nm))
  if (grepl("^ret_", nm))   return(sub("^ret_",   "w_",   nm))
  NA_character_
}

pick_best <- function(group_pattern, cost_suffix = "_c10$") {
  cand <- metrics_all |>
    dplyr::filter(grepl(group_pattern, grupo),
                  grepl(cost_suffix, serie))
  if (!nrow(cand)) cand <- metrics_all |>
      dplyr::filter(grepl(group_pattern, grupo))
  if (!nrow(cand)) return(NA_character_)
  cand$serie[which.max(cand$sharpe)]
}

best_vt <- pick_best("^VT_c/ct")
best_ew <- pick_best("^VT_EWMA_c/ct")
bh_name <- "ret_bh"
stopifnot(bh_name %in% names(ret_bench))
series_pick <- unique(na.omit(c(bh_name, best_vt, best_ew)))
if (length(series_pick) < 2) {
  vt_any <- grep("^ret_", names(ret_cost), value = TRUE)
  series_pick <- unique(c(bh_name, head(vt_any, 1)))
}

# Curvas de capital
df_eq <- tibble::tibble(data = base_ext$data)
for (nm in series_pick) df_eq[[nm]] <- eq_idx(get_series(nm))
df_long <- tidyr::pivot_longer(df_eq, -data, names_to = "serie", values_to = "idx")
p_eq <- ggplot(df_long, aes(x = data, y = idx, color = serie)) +
  geom_line(linewidth = 0.8) +
  labs(title = "Curvas de capital (base 1)", x = NULL, y = "Índice acumulado") +
  theme_minimal(base_size = 11) + theme(legend.title = element_blank())
ggsave(file.path(fig_dir, "equity_curves.png"), p_eq, width = 10, height = 5, dpi = 120)

# Volatilidade rolling 63d
df_vol <- tibble::tibble(data = base_ext$data)
for (nm in series_pick) df_vol[[nm]] <- roll_vol_63(get_series(nm))
dfv_long <- tidyr::pivot_longer(df_vol, -data, names_to = "serie", values_to = "vol_a")
p_vol <- ggplot(dfv_long, aes(x = data, y = vol_a, color = serie)) +
  geom_line(linewidth = 0.7) +
  labs(title = "Volatilidade anualizada rolling (63 dias)", x = NULL, y = "Vol 63d (a.a.)") +
  theme_minimal(base_size = 11) + theme(legend.title = element_blank())
ggsave(file.path(fig_dir, "vol_rolling_63d.png"), p_vol, width = 10, height = 5, dpi = 120)

# Pesos da melhor VT
best_vt_w <- weight_name_from_ret(best_vt)
if (!is.na(best_vt_w) && best_vt_w %in% names(weights_df)) {
  cap_num <- {
    m <- regmatches(best_vt_w, regexpr("cap(\\d+)", best_vt_w))
    ifelse(length(m) && nchar(m) > 0, as.numeric(sub("cap", "", m))/10, NA_real_)
  }
  df_w <- tibble::tibble(data = weights_df$data, w = weights_df[[best_vt_w]])
  p_w <- ggplot(df_w, aes(x = data, y = w)) +
    geom_line(linewidth = 0.7, color = "#2C7FB8") +
    { if (is.finite(cap_num)) geom_hline(yintercept = cap_num, linetype = 2) else NULL } +
    labs(title = paste0("Peso da estratégia: ", best_vt_w), x = NULL, y = "w_t") +
    theme_minimal(base_size = 11)
  ggsave(file.path(fig_dir, "weights_best_vt.png"), p_w, width = 10, height = 4.5, dpi = 120)
}

# Drawdown BH vs melhor VT
dd_from_ret <- function(r) { idx <- eq_idx(r); idx / cummax(idx) - 1 }
df_dd <- tibble::tibble(data = base_ext$data, BH = dd_from_ret(get_series(bh_name)))
if (!is.na(best_vt)) df_dd$VT <- dd_from_ret(get_series(best_vt))
dfdd_long <- tidyr::pivot_longer(df_dd, -data, names_to = "serie", values_to = "dd")
p_dd <- ggplot(dfdd_long, aes(x = data, y = dd, color = serie)) +
  geom_line(linewidth = 0.7) +
  labs(title = "Drawdown ao longo do tempo", x = NULL, y = "DD") +
  scale_y_continuous(labels = scales::percent) +
  theme_minimal(base_size = 11) + theme(legend.title = element_blank())
ggsave(file.path(fig_dir, "drawdowns.png"), p_dd, width = 10, height = 5, dpi = 120)

# ---- Robustez: EGARCH e GJR (Normal e t) -> SAÍDAS ----
# Usa ret_all, dates, n.start, cfg_garch e extract_sigma/.roll_try já definidos acima.

if (!requireNamespace("rugarch", quietly = TRUE)) {
  install.packages("rugarch", dependencies = TRUE, repos = "https://cran.rstudio.com")
}
suppressPackageStartupMessages(library(rugarch))

.egarch_spec <- function(dist = c("norm","std")) {
  dist <- match.arg(dist)
  ugarchspec(
    variance.model = list(model = "eGARCH", garchOrder = c(1,1)),
    mean.model     = list(armaOrder = c(0,0), include.mean = FALSE),
    distribution.model = dist
  )
}
.gjr_spec <- function(dist = c("norm","std")) {
  dist <- match.arg(dist)
  ugarchspec(
    variance.model = list(model = "gjrGARCH", garchOrder = c(1,1)),
    mean.model     = list(armaOrder = c(0,0), include.mean = FALSE),
    distribution.model = dist
  )
}

# Rola previsões fora-da-amostra
specs <- list(
  eg_norm = .egarch_spec("norm"),
  eg_std  = .egarch_spec("std"),
  gj_norm = .gjr_spec("norm"),
  gj_std  = .gjr_spec("std")
)

# Se .roll_try não existir, define-o
if (!exists(".roll_try")) {
  .roll_try <- function(spec) {
    try(
      ugarchroll(
        spec, data = ret_all, n.ahead = 1,
        n.start = n.start,
        refit.every = cfg_garch$refit_every %||% 5L,
        refit.window = "expanding",
        calculate.VaR = FALSE, keep.coef = TRUE,
        solver = "hybrid",
        parallel = isTRUE(cfg_garch$parallel)
      ),
      silent = TRUE
    )
  }
}
# Se extract_sigma não existir, define-o
if (!exists("extract_sigma")) {
  extract_sigma <- function(roll_obj) {
    df <- try(as.data.frame(roll_obj), silent = TRUE)
    if (inherits(df, "try-error")) return(NULL)
    idx <- suppressWarnings(as.Date(df$Index))
    if (all(is.na(idx))) idx <- suppressWarnings(as.Date(rownames(df)))
    if (all(is.na(idx))) return(NULL)
    tibble::tibble(data = idx, Sigma = as.numeric(df$Sigma))
  }
}

res <- lapply(specs, .roll_try)
sig <- lapply(res, extract_sigma)

# Fallback lento se necessário
need_fallback <- all(vapply(sig, is.null, logical(1)))
if (need_fallback) {
  cat("ugarchroll indisponível. Fallback lento para EGARCH/GJR.\n")
  slow_one <- function(spec) {
    origins <- dates[(n.start + 1L):length(dates)]
    out <- vector("list", length(origins))
    for (k in seq_along(origins)) {
      t0 <- origins[k]; idx <- which(dates < t0)
      if (length(idx) < 250) { out[[k]] <- tibble::tibble(data = t0, Sigma = NA_real_); next }
      x_tr <- ret_all[idx]
      fit  <- try(ugarchfit(spec, data = x_tr, solver = "hybrid", solver.control = list(trace = 0)), silent = TRUE)
      f    <- try(if (!inherits(fit, "try-error")) ugarchforecast(fit, n.ahead = 1) else NULL, silent = TRUE)
      s    <- if (!inherits(f, "try-error") && !is.null(f)) as.numeric(sigma(f))[1] else NA_real_
      out[[k]] <- tibble::tibble(data = t0, Sigma = s)
      if (k %% 250 == 0) cat(sprintf("Fallback %d/%d...\n", k, length(origins)))
    }
    dplyr::bind_rows(out)
  }
  sig <- list(
    eg_norm = slow_one(.egarch_spec("norm")),
    eg_std  = slow_one(.egarch_spec("std")),
    gj_norm = slow_one(.gjr_spec("norm")),
    gj_std  = slow_one(.gjr_spec("std"))
  )
}

# Consolida previsões
dates_oos <- dates[(n.start + 1L):length(dates)]
vol_robust <- tibble::tibble(data = dates_oos)
add_col <- function(tbl, x, nm) if (!is.null(x)) dplyr::left_join(tbl, dplyr::rename(x, !!nm := Sigma), by = "data") else tbl
vol_robust <- add_col(vol_robust, sig$eg_norm, "sigma_egarch_norm")
vol_robust <- add_col(vol_robust, sig$eg_std,  "sigma_egarch_std")
vol_robust <- add_col(vol_robust, sig$gj_norm, "sigma_gjr_norm")
vol_robust <- add_col(vol_robust, sig$gj_std,  "sigma_gjr_std")

# Salva previsões EGARCH/GJR
readr::write_rds(vol_robust, "saídas/dados/vol_forecasts_egarch_gjr.rds")
readr::write_csv(vol_robust, "saídas/dados/vol_forecasts_egarch_gjr.csv")

# Anexa à base e regrava uma versão estendida
base_with_sigmas <- base_with_sigmas |>
  dplyr::left_join(vol_robust, by = "data")

readr::write_rds(base_with_sigmas, "saídas/dados/bova11_diario_with_sigmas_all.rds")
readr::write_csv(base_with_sigmas, "saídas/dados/bova11_diario_with_sigmas_all.csv")

cat("EGARCH/GJR salvos em saídas/dados/vol_forecasts_egarch_gjr.{rds,csv} e anexados à base.\n")

# ---- Integração EGARCH/GJR nos pesos, OOS estrito, testes e seleção final -> SAÍDAS ----

# Config extras
if (!exists("cfg")) cfg <- list()
if (is.null(cfg$max_leverage))   cfg$max_leverage <- max(cfg$cap %||% 1.5)  # limite duro de alavancagem
if (is.null(cfg$dd_threshold))   cfg$dd_threshold <- NA_real_               # ex.: 0.20 para de-risk opcional
if (is.null(cfg$dd_scale))       cfg$dd_scale     <- 0.5                    # escala de-risk quando DD cruza limiar

# Helpers
vol_a_to_d <- function(x, days = 252) x / sqrt(days)
mk_name2 <- function(lbl, st_d, cap) {
  tgt_a <- round(st_d * sqrt(252) * 100)
  sprintf("%s_%02da_cap%s", lbl, tgt_a, gsub("\\.", "", as.character(cap)))
}
smooth_clip <- function(s, k = cfg$smooth_k, lo = cfg$sigma_floor_d, hi = cfg$sigma_ceiling_d) {
  if (is.numeric(k) && k >= 3 && (k %% 2 == 1)) s <- stats::runmed(s, k = k, endrule = "median")
  pmin(pmax(s, lo), hi)
}
apply_risk_controls <- function(w, dd_series, max_lev = cfg$max_leverage, dd_th = cfg$dd_threshold, dd_scale = cfg$dd_scale) {
  w <- pmin(w, max_lev)
  if (is.finite(dd_th) && !all(is.na(dd_series))) {
    scale_vec <- ifelse(dd_series <= -abs(dd_th), dd_scale, 1)
    w <- w * scale_vec
  }
  w
}

# Seleciona sigmas adicionais disponíveis (EGARCH/GJR)
sigma_cols_avail <- intersect(
  c("sigma_egarch_norm","sigma_egarch_std","sigma_gjr_norm","sigma_gjr_std"),
  names(base_with_sigmas)
)
label_map <- c(
  sigma_egarch_norm = "w_egarch_norm",
  sigma_egarch_std  = "w_egarch_std",
  sigma_gjr_norm    = "w_gjr_norm",
  sigma_gjr_std     = "w_gjr_std"
)

# Preparação
sigma_targets_d <- vol_a_to_d(cfg$sigma_targets)
dd_series <- base_ext$dd

# Expande weights_df/ret_df/turnover/ret_cost com variantes de sigma
for (scol in sigma_cols_avail) {
  sigma_sm <- smooth_clip(base_with_sigmas[[scol]])
  lbl_base <- label_map[[scol]]
  
  for (st_d in sigma_targets_d) {
    for (cp in cfg$cap) {
      nm_core <- mk_name2(lbl_base, st_d, cp)                # ex.: w_egarch_norm_15a_cap15
      w_raw   <- pmin(cp, st_d / sigma_sm)
      w_raw[!is.finite(w_raw)] <- 1
      w_adj   <- apply_risk_controls(w_raw, dd_series)
      
      # Pesos
      if (!nm_core %in% names(weights_df)) weights_df[[nm_core]] <- w_adj
      
      # Turnover
      to_name <- sub("^w_", "to_", nm_core)
      turnover[[to_name]] <- c(0, abs(diff(w_adj)))
      
      # Retornos grossos e com custos
      ret_name <- sub("^w_", "ret_", nm_core)
      if (!ret_name %in% names(ret_df)) {
        r <- w_adj * ret_base$ret_bova11
        r[!is.finite(r)] <- 0
        ret_df[[ret_name]] <- r
      }
      for (cbps in cfg$cost_bps) {
        nm_out <- sprintf("%s_c%02d", ret_name, cbps)
        cost_series <- (cbps / 1e4) * turnover[[to_name]]
        r_net <- ret_df[[ret_name]] - cost_series
        r_net[!is.finite(r_net)] <- 0
        ret_cost[[nm_out]] <- r_net
      }
    }
  }
}

# Persistência atualizada
readr::write_rds(weights_df, "saídas/dados/vt_pesos.rds");           readr::write_csv(weights_df, "saídas/dados/vt_pesos.csv")
readr::write_rds(turnover,   "saídas/dados/vt_turnover.rds");        readr::write_csv(turnover,   "saídas/dados/vt_turnover.csv")
readr::write_rds(ret_df,     "saídas/dados/vt_retornos_sem_custos.rds")
readr::write_csv(ret_df,     "saídas/dados/vt_retornos_sem_custos.csv")
readr::write_rds(ret_cost,   "saídas/dados/vt_retornos_com_custos.rds")
readr::write_csv(ret_cost,   "saídas/dados/vt_retornos_com_custos.csv")

# ---- Backtest estrito OOS: usa peso defasado (w_{t-1}) ----
ret_df_oos   <- tibble::tibble(data = ret_base$data)
ret_cost_oos <- tibble::tibble(data = ret_base$data)

ret_cols_now <- grep("^ret_((?!_c\\d+$).)*$", names(ret_df), perl = TRUE, value = TRUE)  # sem _cXX
for (nm in ret_cols_now) {
  wnm <- sub("^ret_", "w_", nm)
  ton <- sub("^ret_", "to_", nm)
  w_series <- dplyr::coalesce(dplyr::lag(weights_df[[wnm]]), 0)  # defasa 1
  r_oos <- w_series * ret_base$ret_bova11
  r_oos[!is.finite(r_oos)] <- 0
  ret_df_oos[[nm]] <- r_oos
  
  # custos iguais aos síncronos (turnover já usa variação de w_t)
  for (cbps in cfg$cost_bps) {
    nm_out <- sprintf("%s_c%02d", nm, cbps)
    cost_series <- (cbps / 1e4) * turnover[[ton]]
    ret_cost_oos[[nm_out]] <- r_oos - cost_series
  }
}

readr::write_rds(ret_df_oos,   "saídas/dados/vt_retornos_oos_sem_custos.rds")
readr::write_csv(ret_df_oos,   "saídas/dados/vt_retornos_oos_sem_custos.csv")
readr::write_rds(ret_cost_oos, "saídas/dados/vt_retornos_oos_com_custos.rds")
readr::write_csv(ret_cost_oos, "saídas/dados/vt_retornos_oos_com_custos.csv")

# ---- Métricas completas (recalcula e sobrescreve) ----
ann_ret <- function(x) exp(mean(x, na.rm = TRUE) * 252) - 1
ann_vol <- function(x) stats::sd(x, na.rm = TRUE) * sqrt(252)
sharpe  <- function(x) mean(x, na.rm = TRUE) / stats::sd(x, na.rm = TRUE) * sqrt(252)
sortino <- function(x) { dn <- x[x < 0]; if (!length(dn)) return(Inf); mean(x, na.rm = TRUE) * 252 / (stats::sd(dn, na.rm = TRUE) * sqrt(252)) }
mdd     <- function(x) { idx <- exp(cumsum(replace(x, !is.finite(x), 0))); min(idx / cummax(idx) - 1, na.rm = TRUE) }
calmar  <- function(x) { cagr <- ann_ret(x); d <- abs(mdd(x)); if (d == 0) Inf else cagr / d }

make_metrics <- function(df, cols, rf_series = NULL, tag = "") {
  dplyr::bind_rows(lapply(cols, function(nm) {
    x <- df[[nm]]
    tibble::tibble(
      serie    = nm,
      ann_ret  = ann_ret(x),
      ann_vol  = ann_vol(x),
      sharpe   = sharpe(if (is.null(rf_series)) x else x - rf_series),
      sortino  = sortino(x),
      mdd      = mdd(x),
      calmar   = calmar(x),
      n        = sum(is.finite(x)),
      grupo    = tag
    )
  }))
}

rf_vec <- dplyr::coalesce(base_ext$rf_diaria, 0)
vt_cols_nc  <- grep("^ret_", names(ret_df), value = TRUE)
vt_cols_c   <- grep("^ret_.*_c\\d+$", names(ret_cost), value = TRUE)
vt_oos_nc   <- grep("^ret_", names(ret_df_oos), value = TRUE)
vt_oos_c    <- grep("^ret_.*_c\\d+$", names(ret_cost_oos), value = TRUE)
bench_cols  <- c("ret_bh")

met_bench   <- make_metrics(ret_bench, bench_cols, rf_vec, "BH")
met_sync_nc <- make_metrics(ret_df,    vt_cols_nc, rf_vec, "VT_sync_s/ct")
met_sync_c  <- make_metrics(ret_cost,  vt_cols_c,  rf_vec, "VT_sync_c/ct")
met_oos_nc  <- make_metrics(ret_df_oos,   vt_oos_nc, rf_vec, "VT_OOS_s/ct")
met_oos_c   <- make_metrics(ret_cost_oos, vt_oos_c,  rf_vec, "VT_OOS_c/ct")

metrics_all <- dplyr::bind_rows(met_bench, met_sync_nc, met_sync_c, met_oos_nc, met_oos_c) |>
  dplyr::arrange(desc(sharpe))
readr::write_rds(metrics_all, "saídas/dados/metrics_all.rds")
readr::write_csv(metrics_all, "saídas/dados/metrics_all.csv")

# ---- Teste de Sharpe (Jobson–Korkie + Memmel) vs BH em OOS c/ 10 bps ----
jk_memmel <- function(x, y) {
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

sel_oos_c10 <- grep("_c10$", names(ret_cost_oos), value = TRUE)
jk_rows <- lapply(sel_oos_c10, function(nm) {
  res <- jk_memmel(ret_cost_oos[[nm]], ret_bench$ret_bh)
  cbind.data.frame(serie_vs_bh = nm, z = res$z, p_two_sided = res$p_two_sided)
})
jk_tbl <- dplyr::bind_rows(jk_rows)
dir.create("saídas/tabelas", recursive = TRUE, showWarnings = FALSE)
readr::write_csv(jk_tbl, "saídas/tabelas/sharpe_tests_jk_memmel_vs_bh.csv")

# ---- Janelas de estresse (COVID + regimes SELIC) em OOS c/ 10 bps ----
rf_aa <- (1 + rf_vec)^252 - 1
regime_selic <- cut(rf_aa,
                    breaks = c(-Inf, 0.06, 0.10, Inf),
                    labels = c("Baixa(<=6%)", "Média(6–10%)", "Alta(>10%)"),
                    right = TRUE
)
dates <- base_ext$data
mask_covid <- dates >= as.Date("2020-02-20") & dates <= as.Date("2020-09-30")

metrics_by_mask <- function(cols, mask, label) {
  dplyr::bind_rows(lapply(cols, function(nm) {
    x <- if (nm == "ret_bh") ret_bench[[nm]] else ret_cost_oos[[nm]]
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
met_covid <- metrics_by_mask(sel_cols, mask_covid, "COVID(2020-02-20..2020-09-30)")
met_regimes <- dplyr::bind_rows(lapply(levels(regime_selic), function(lv) {
  metrics_by_mask(sel_cols, regime_selic == lv, paste0("SELIC_", lv))
}))
metrics_stress <- dplyr::bind_rows(met_covid, met_regimes) |>
  dplyr::arrange(grupo, dplyr::desc(sharpe))
readr::write_csv(metrics_stress, "saídas/tabelas/metrics_stress_windows.csv")

# ---- Seleção final do modelo (OOS, 10 bps) ----
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

# ---- Reprodutibilidade: cfg + sessionInfo ----
if (!requireNamespace("jsonlite", quietly = TRUE)) {
  install.packages("jsonlite", dependencies = TRUE, repos = "https://cran.rstudio.com")
}
suppressPackageStartupMessages(library(jsonlite))
try(write(jsonlite::toJSON(cfg, pretty = TRUE, auto_unbox = TRUE), "saídas/dados/config_vt.json"))
sess <- capture.output(utils::sessionInfo())
writeLines(sess, "saídas/tabelas/session_info.txt")