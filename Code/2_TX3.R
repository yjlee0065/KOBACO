
library(dplyr)
library(lubridate)
library(stringr)

#-------------------------------------------------------------------------------
# TX3(변환 데이터) 파일 로드 및 파싱
#-------------------------------------------------------------------------------

#' TX3 혹은 수정된 시청기록 텍스트 파일을 읽어 가구별 시청 세션 리스트로 변환하는 함수
#' @param file_path 읽어올 파일 경로 (.tx3 혹은 .txt)
#' @return 가구 ID를 이름으로 가진 시청 기록 리스트

load_tx3 <- function(file_path) {
  file_name <- basename(file_path)
  
  # 파일 이름에서 날짜 추출
  # 파일명에서 숫자 8자리를 찾아 날짜로 인식
  date <- sub("^(\\d{4})(\\d{2})(\\d{2}).*", "\\1\\2\\3", file_name)
  
  lines <- readLines(file_path)
  all_results <- list()
  current_block <- list()
  
  # 각 가구의 V 라인들을 데이터프레임으로 변환
  v_data <- function(id, v_lines) {
    result <- list()
    
    for (line in v_lines) {
      if (grepl("^V", line)) {
        chn <- as.numeric(sub("^V(\\d+)_.*", "\\1", line))
        
        # 시간 구간 추출
        # 예: ab233300240359 -> aa(2), start(6), end(6)
        matches <- regmatches(line, gregexpr("([a-zA-Z]{2})(\\d{6})(\\d{6})", line))[[1]]
        
        for (match in matches) {
          start <- substr(match, 3, 8)
          end <- substr(match, 9, 14)
          result <- append(result, list(c(as.numeric(id), chn, start, end, date)))
        }
      }
    }
    
    if (length(result) > 0) {
      df <- as.data.frame(do.call(rbind, result), stringsAsFactors = FALSE)
      colnames(df) <- c("id", "chn", "start", "end", "date")
      df$id <- as.numeric(df$id)
      return(df)
    } else {
      return(NULL)
    }
  }
  
  for (line in lines) {
    if (grepl("^H", line)) {
      # 이전 가구 블록 처리
      if (length(current_block) > 0) {
        id <- as.numeric(str_extract(current_block[1], "\\d+"))
        df <- v_data(id, current_block[-1])
        
        # V 기록이 없는 경우에도 가구 정보는 유지 (NA 처리)
        if (is.null(df)) {
          # V 행이 없는 경우, 빈 데이터프레임 생성
          df <- data.frame(
            id = id,
            ariana_code = NA_character_,
            start = NA_character_,
            end = NA_character_,
            date = date,
            stringsAsFactors = FALSE
          )
        }
        all_results[[as.character(id)]] <- df
      }
      current_block <- list(line)
    } else if (nchar(line) > 0) {
      current_block <- append(current_block, line)
    }
  }
  
  # 마지막 가구 블록 처리
  if (length(current_block) > 0) {
    id <- as.numeric(str_extract(current_block[1], "\\d+"))
    df <- v_data(id, current_block[-1])
    if (is.null(df)) {
      # V 행이 없는 경우, 빈 데이터프레임 생성
      df <- data.frame(
        id = id,
        ariana_code = NA_character_,
        start = NA_character_,
        end = NA_character_,
        date = date,
        stringsAsFactors = FALSE
      )
    }
    all_results[[as.character(id)]] <- df
  }
  
  output_df <- bind_rows(all_results)
  
  output_df <- output_df %>%
    filter(!chn %in% c(65535, NA))
  
  return(output_df)
}

path <- 'C:/D/공부/대학원/연구실/프로젝트/닐슨/포폴용 코드/20240101.txt'
tx3 <- load_tx3(path)
