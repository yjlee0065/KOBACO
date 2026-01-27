################################################################################
# 프로젝트: 시청률 조사 데이터 검증 및 편집 규칙(Validation Rule) 구현
# 목적: 미터기 원시 데이터(Raw Log)를 데이터프레임화하고 'Uncovered Viewing' 규칙 적용
# 작성일: 2025-01-24
################################################################################

library(stringr)
library(dplyr)
library(lubridate)

#-----------------------------------------------------------------------------
# 1. 패널 가구별 원시 시청 기록 추출 (Log Parsing)
#-----------------------------------------------------------------------------

log_data <- function(file_path, date) {
  
  # 원시 데이터 로드 [cite: 151, 193]
  raw_logs <- readLines(file_path)
  
  panel_data_list <- list()
  current_panel <- NULL
  temp_log <- list()
  
  for (line in raw_logs) {
    # 패널 가구 정보 행 감지 (예: - 01/01/2024 panel 1001 ...)
    if (str_detect(line, "panel \\d+")) {
      
      if (!is.null(current_panel) && length(temp_log) > 0) {
        panel_df <- bind_rows(temp_log) %>% 
          mutate(id = current_panel, date = date) %>% 
          select(id, time, act, chn, date)
        panel_data_list[[as.character(current_panel)]] <- panel_df
      }
      
      new_panel <- str_extract(line, "panel \\d+")
      current_panel <- as.numeric(str_extract(new_panel, "\\d+"))
      temp_log <- list()
      
    } 
  
    else if (str_detect(line, "^\\d{2}:\\d{2}:\\d{2}")) {
      # 시청 로그 행 분석 (HH:MM:SS)
      time_val <- str_extract(line, "^\\d{2}:\\d{2}:\\d{2}")
      # 채널 번호 추출 (Chn XXX)
      chn_val <- ifelse(str_detect(line, "Chn \\d+"), 
                        as.numeric(str_extract(line, "(?<=Chn )\\d+")), NA)
      # 액션 정보 추출 (member entered, member exited)
      act_val <- str_match(line, "BU \\d{2} : (.+?)(?= on| has| IRKEY|$)")[,2]
      
      # 가구원(Member) 시청 여부 및 입퇴장 상태 확인
      if (str_detect(line, "member \\d+ has (entered|exited)")) {
        member_match <- str_match(line, "member (\\d+) has (entered|exited)")
        act_val <- paste("member", member_match[,2], "has", member_match[,3])
      }
      
      # 게스트(Guest) 시청 상태 확인 
      if (str_detect(line, "guest \\d+ has (entered|exited)")) {
        guest_match <- str_match(line, "guest (\\d+) has (entered|exited)")
        act_val <- paste("guest", guest_match[,2], "has", guest_match[,3])
      }
      
      # 개별 로그 데이터를 임시 저장
      temp_log <- append(temp_log, list(data.frame(
        time = time_val,
        chn = chn_val,
        act = act_val,
        stringsAsFactors = FALSE
      )))
    }
  }
  
  # 마지막 패널 데이터 추가
  if (!is.null(current_panel) && length(temp_log) > 0) {
    panel_df <- bind_rows(temp_log) %>%
      mutate(id = current_panel, date = date) %>%
      select(id, time, act, chn, date)
    panel_data_list[[as.character(current_panel)]] <- panel_df
  }
  return(panel_data_list)
}

# 경로 추후 수정
path <- 'C:/D/공부/대학원/연구실/프로젝트/닐슨/포폴용 코드/MET20240101.txt'
met_step1 <- log_data(path, 20240101)

#-------------------------------------------------------------------------------
# 2. 게스트 전용 시청 가구 제외
#-------------------------------------------------------------------------------

#' 조사 회사의 규정에 따라 가구원(Member) 없이 게스트(Guest)만 시청한 기록은 분석에서 제외함 [cite: 154, 155]
#' @param panel_list 1단계에서 추출된 패널별 시청 기록 리스트
#' @return 게스트 전용 시청 가구가 제거된 리스트
remove_guest_only_ids <- function(panel_list) {
  cleaned_list <- list()
  
  for (i in seq_along(panel_list)) {
    current_df <- panel_list[[i]]
    
    # act 열이 존재하는 경우에만 필터링 로직 수행
    if ("act" %in% colnames(current_df)) {
      
      # 해당 가구 데이터 내에서 member와 guest의 포함 여부 확인
      member_ids <- unique(current_df$id[grepl("member", current_df$act, ignore.case = TRUE)])
      guest_ids <- unique(current_df$id[grepl("guest", current_df$act, ignore.case = TRUE)])
      
      # member 기록은 없고 guest 기록만 존재하는 ID 식별
      guest_only_ids <- setdiff(guest_ids, member_ids)
      
      # guest 기록만 있는 ID에 해당하지 않는 행만 유지
      filtered_df <- current_df %>% filter(!(id %in% guest_only_ids))
      
      if (nrow(filtered_df) > 0) {
        cleaned_list[[names(panel_list)[i]]] <- filtered_df
      }
    } else {
      # act 열이 없는 경우 원본 유지
      if (nrow(current_df) > 0) {
        cleaned_list[[names(panel_list)[i]]] <- current_df
      }
    }
  }
  
  return(cleaned_list)
}

# guest 필터링 적용
# 1단계 결과(met_step1)를 입력으로 사용
met_step2 <- remove_guest_only_ids(met_step1)

# 결과 확인
cat("필터링 전 가구 수:", length(met_step1), "\n")
cat("필터링 후 유효 가구 수:", length(met_step2), "\n")

#-------------------------------------------------------------------------------
# 3. 연속된 멤버 입퇴장 기록 병합 (Member Transition Noise Filtering)
#-------------------------------------------------------------------------------

#' 동일 가구원이 짧은 시간(5초 이내) 내에 퇴장 후 재입장하는 경우, 
#' 이를 하나의 연속된 시청 세션으로 간주하여 데이터 노이즈를 제거하는 함수 
#' @param panel_list 2단계 게스트 필터링이 완료된 패널 리스트 (met_step1)
#' @return 단기 입퇴장 기록이 병합된 패널 리스트
merge_member_transitions <- function(panel_list) {
  merged_list <- list()
  
  for (i in seq_along(panel_list)) {
    current_df <- panel_list[[i]]
    
    # 시간 계산을 위해 HMS 형식의 보조 컬럼 생성
    current_df <- current_df %>%
      mutate(time_hms = hms(time))
    
    # 삭제할 행 번호를 저장할 벡터
    rows_to_remove <- c()
    
    # 데이터프레임의 행을 순회하며 연속된 입퇴장 패턴 확인
    # 루프 범위는 1부터 (전체 행 수 - 1)까지
    if (nrow(current_df) > 1) {
      for (j in 1:(nrow(current_df) - 1)) {
        current_act <- current_df$act[j]
        next_act <- current_df$act[j + 1]
        
        # 동일 가구원이 퇴장(exited) 후 바로 다음 행에서 입장(entered)하는 경우
        if (grepl("member \\d+ has exited", current_act) && grepl("member \\d+ has entered", next_act)) {
          current_member <- str_extract(current_act, "member \\d+")
          next_member <- str_extract(next_act, "member \\d+")
          
          # 동일 인물인지 확인 후 시간 차이 계산
          if (!is.na(current_member) && !is.na(next_member) && current_member == next_member) {
            time_gap <- as.numeric(as.period(current_df$time_hms[j + 1] - current_df$time_hms[j], unit = "sec"))
            
            # 5초 이내의 짧은 전환인 경우 해당 퇴장/입장 행을 삭제 대상으로 지정
            if (time_gap <= 5) {
              rows_to_remove <- c(rows_to_remove, j, j + 1)
            }
          }
        }
      }
    }
    
    rows_to_remove <- unique(rows_to_remove)
    
    # 삭제 대상이 있는 경우 해당 행 제외 처리
    if (length(rows_to_remove) > 0) {
      cleaned_df <- current_df[-rows_to_remove, ]
    } else {
      cleaned_df <- current_df
    }
    
    # time_hms 컬럼 제거 후 결과 리스트에 저장
    merged_list[[names(panel_list)[i]]] <- cleaned_df %>%
      select(-time_hms)
  }
  
  return(merged_list)
}

# 2단계 결과 met_step2을 인풋으로 사용하여 입퇴장 병합 수행
met_step3 <- merge_member_transitions(met_step2)

#-------------------------------------------------------------------------------
# 4. 1분 단위 시청기록 재정리
#-------------------------------------------------------------------------------

#' 1초 단위의 미터기 데이터를 분석 표준인 1분 단위(TX3 형식)로 변환하는 보조 함수 
#' @param time_vector HH:MM:SS 형식의 시간 벡터
#' @return 30초 기준 반올림이 적용된 시간 벡터

adjust_time <- function(panel_list) {
  processed_list <- list()
  
  for (panel_id in names(panel_list)) {
    current_df <- panel_list[[panel_id]]
    
    current_df$time <- sapply(current_df$time, function(t) {
      # 시간 문자열 분리
      parts <- as.numeric(unlist(strsplit(t, ":")))
      h <- parts[1]
      m <- parts[2]
      s <- parts[3]
      
      # 30초 미만(0-29초)은 분 유지, 30초 이상(30-59초)은 1분 올림 
      if (s >= 30) {
        m <- m + 1
        s <- 0
      } else {
        s <- 0
      }
      # 60분 도달 시 시간(hour) 단위 올림 처리
      if (m == 60) {
        m <- 0
        h <- h + 1
      }
      # 시간 문자열로 변환
      sprintf("%02d:%02d:%02d", h, m, s)
    })
    processed_list[[panel_id]] <- current_df
  }
  
  return(processed_list)
}

# 3단계 결과 met_step3을 인풋으로 사용하여 4단계 시간 재정리 수행
met_step4 <- adjust_time(met_step3)

# 결과 확인 예시: 초(second) 단위가 "00"으로 정렬되었는지 확인
# head(met_step3[[1]])

#-------------------------------------------------------------------------------
# 5. Uncovered Viewing
#-------------------------------------------------------------------------------

# 시간 차이를 초 단위로 계산하는 함수
calculate_time_diff <- function(time1, time2) {
  time1_split <- as.numeric(unlist(strsplit(time1, ":")))
  time2_split <- as.numeric(unlist(strsplit(time2, ":")))
  
  # HH:MM:SS를 총 초 단위로 변환
  seconds1 <- time1_split[1] * 3600 + time1_split[2] * 60 + time1_split[3]
  seconds2 <- time2_split[1] * 3600 + time2_split[2] * 60 + time2_split[3]
  
  return(seconds2 - seconds1)
}

# 4단계 결과 met_step4를 인풋으로 사용
met_step4_processed <- list() # 편집 규칙 적용 결과 임시 저장 리스트

for (i in seq_along(met_step4)) {
  # 현재 패널 데이터프레임 추출
  current_df <- met_step4[[i]]
  panel_id <- unique(current_df$id)
  
  # 유효한 ID가 없는 경우 스킵
  if (length(panel_id) == 0 || is.na(panel_id) || is.null(panel_id)) {
    next
  }
  
  ##### 1. 전체 TV On 시간(Total Time) 계산 #####
  # TV가 켜진 시점부터 "Off" 로그가 발생한 시점까지의 총합
  total_time <- 0
  total_prev_idx <- 1
  
  # "Off"가 포함된 행 찾기
  off_event_rows <- which(grepl("Off", current_df$act, ignore.case = TRUE))
  
  # 각 "Off" 이벤트까지의 시간 차이를 누적
  for (idx in off_event_rows) {
    if (total_prev_idx < idx) {
      start_t <- current_df$time[total_prev_idx]
      end_t <- current_df$time[idx]
      total_time <- total_time + calculate_time_diff(start_t, end_t)
    }
    total_prev_idx <- idx + 1
  }
  
  ##### 2. 가구원 유효 시청 시간 계산 #####
  # 실시간 시청으로 인정되는 멤버 입장/퇴장 사이의 시간 합산
  viewing_time <- 0
  viewing_prev_idx <- 1
  
  # 각 "Off" 세션 내에서 핸드셋 조작 구간 확인
  for (idx in off_event_rows) {
    if (viewing_prev_idx < idx) {
      # 세션 단위 데이터 분리
      session_chunk <- current_df[viewing_prev_idx:idx, ]
      
      # 해당 세션 내 멤버 입장/퇴장 로그 인덱스 추출
      enter_idxs <- which(grepl("has entered", session_chunk$act))
      exit_idxs <- which(grepl("exited", session_chunk$act))
      
      # 입장과 퇴장 기록이 모두 있는 경우 유효 조작 구간으로 인정
      if (length(enter_idxs) > 0 && length(exit_idxs) > 0) {
        first_enter_t <- session_chunk$time[enter_idxs[1]]
        last_exit_t <- session_chunk$time[exit_idxs[length(exit_idxs)]]
        
        viewing_time <- viewing_time + calculate_time_diff(first_enter_t, last_exit_t)
      }
    }
    viewing_prev_idx <- idx + 1
  }
  
  ##### 3. 50% 편집 규칙 적용 및 부적격 가구 마킹 #####
  # 핸드셋 조작 시간이 전체 시청 시간의 50% 이하이면 가구 제외 대상으로 분류
  if (total_time > 0) {
    viewing_ratio <- viewing_time / total_time
    
    if (viewing_ratio <= 0.5) {
      # 부적격 가구는 모든 행을 삭제하고 'UV' 식별자로 대체
      current_df <- current_df[0, ] 
      current_df <- data.frame(
        id = 'UV',
        time = NA,
        act = NA,
        chn = NA,
        date = NA
      )
    }
  }
  
  # 결과 리스트에 패널 ID를 키값으로 저장
  met_step4_processed[[as.character(panel_id)]] <- current_df
}

#### 4. 부적격(UV) 가구 최종 필터링 및 리스트 생성 ####
# 'UV'로 마킹된 가구 ID 추출
uv_marked_panels <- names(met_step4_processed)[sapply(met_step4_processed, function(df) any(df$id == "UV"))]

# 부적격 가구를 제외한 최종 유효 가구 리스트(met_step5) 생성
met_step5 <- met_step4_processed[!(names(met_step4_processed) %in% uv_marked_panels)]

#-------------------------------------------------------------------------------
# 5. Uncovered Viewing (핸드셋 조작 비율 기반 가구 필터링)
#-------------------------------------------------------------------------------

#' 패널 가구가 미터기 버튼 조작에 미숙하거나 비협조적일 경우 발생하는 데이터 왜곡을 방지하기 위해 
#' 전체 시청 시간 중 50% 이상 조작이 없는 가구를 제외하는 함수
#' @param panel_list 4단계 시간 재정리가 완료된 리스트 (met_step4)
#' @return Uncovered Viewing 규칙을 통과한 유효 가구 리스트 (met_step5)
uncovered_viewing <- function(panel_list) {
  
  # 시간 차이를 초 단위로 계산하는 내부 함수
  calculate_time_diff <- function(time1, time2) {
    time1_split <- as.numeric(unlist(strsplit(time1, ":")))
    time2_split <- as.numeric(unlist(strsplit(time2, ":")))
    
    # HH:MM:SS를 총 초 단위로 변환
    seconds1 <- time1_split[1] * 3600 + time1_split[2] * 60 + time1_split[3]
    seconds2 <- time2_split[1] * 3600 + time2_split[2] * 60 + time2_split[3]
    
    return(seconds2 - seconds1)
  }
  
  valid_list <- list() # 유효 가구만 담을 결과 리스트
  
  for (i in seq_along(panel_list)) {
    # 현재 패널 데이터프레임 추출
    current_df <- panel_list[[i]]
    panel_id <- unique(current_df$id)
    
    # 유효한 ID가 없는 경우 스킵
    if (length(panel_id) == 0 || is.na(panel_id) || is.null(panel_id)) {
      next
    }
    
    # 1. 전체 TV On 시간(Total Time) 계산
    # TV가 켜진 시점부터 "Off" 로그가 발생한 시점까지의 총합
    total_time <- 0
    total_prev_idx <- 1
    
    # "Off"가 포함된 행 찾기
    off_event_rows <- which(grepl("Off", current_df$act, ignore.case = TRUE))
    
    for (idx in off_event_rows) {
      if (total_prev_idx < idx) {
        start_t <- current_df$time[total_prev_idx]
        end_t <- current_df$time[idx]
        total_time <- total_time + calculate_time_diff(start_t, end_t)
      }
      total_prev_idx <- idx + 1
    }
    
    # 2. 가구원 유효 시청 시간 계산
    # 실시간 시청으로 인정되는 멤버 입장/퇴장 사이의 시간 합산
    viewing_time <- 0
    viewing_prev_idx <- 1
    
    for (idx in off_event_rows) {
      if (viewing_prev_idx < idx) {
        # 세션 단위 데이터 분리
        session_chunk <- current_df[viewing_prev_idx:idx, ]
        
        # 해당 세션 내 멤버 입장/퇴장 로그 인덱스 추출
        enter_idxs <- which(grepl("has entered", session_chunk$act))
        exit_idxs <- which(grepl("exited", session_chunk$act))
        
        # 입장과 퇴장 기록이 모두 있는 경우 유효 조작 구간으로 인정
        if (length(enter_idxs) > 0 && length(exit_idxs) > 0) {
          first_enter_t <- session_chunk$time[enter_idxs[1]]
          last_exit_t <- session_chunk$time[exit_idxs[length(exit_idxs)]]
          
          viewing_time <- viewing_time + calculate_time_diff(first_enter_t, last_exit_t)
        }
      }
      viewing_prev_idx <- idx + 1
    }
    
    # 3. 50% 편집 규칙 적용 및 직접 필터링
    # 핸드셋 조작 비율이 50%를 초과하는 가구만 결과 리스트에 추가
    if (total_time > 0) {
      viewing_ratio <- viewing_time / total_time
      
      if (viewing_ratio > 0.5) {
        valid_list[[as.character(panel_id)]] <- current_df
      }
    }
  }
  
  return(valid_list)
}

# 4단계 결과(met_step4)를 인풋으로 사용하여 5단계 Uncovered Viewing 함수 실행
met_step5 <- uncovered_viewing(met_step4)

# 결과 요약 출력
cat("검증 전 전체 가구 수:", length(met_step4), "\n")
cat("최종 유효 가구 수(met_step5):", length(met_step5), "\n")
cat("리젝된 가구 수:", length(met_step4) - length(met_step5), "\n")

#-------------------------------------------------------------------------------
# 6. 채널 데이터 정제 및 중복 기록 제거
#-------------------------------------------------------------------------------

#' 멤버 입장 시 채널 정보를 보정하고 유효하지 않은 채널 코드를 처리하는 함수
#' @param panel_list 5단계 Uncovered Viewing 필터링이 완료된 리스트 (met_step5)
#' @return 채널 코드 보정이 완료된 리스트
entered_chn <- function(panel_list) {
  for (i in seq_along(panel_list)) {
    df <- panel_list[[i]] %>%
      mutate(
        # 실질적인 시청 채널이 아닌 코드(0, 65535) 및 녹화 정보 NA 처리
        chn = case_when(
          chn == 0 ~ NA_real_,
          chn == 65535 ~ NA_real_,
          grepl("- Rec.", act) ~ NA_real_,
          TRUE ~ chn
        ),
        # 멤버가 입장(entered)한 시점의 채널은 직전 로그의 채널 정보를 가져옴
        chn = if_else(endsWith(act, "entered"), lag(chn), chn)
      )
    
    # act가 entered이고 chn이 NA인 행의 chn 값을 업데이트
    for (j in seq_len(nrow(df))) {
      if (endsWith(df$act[j], "entered") && is.na(df$chn[j])) {
        if (j > 1 && endsWith(df$act[j - 1], "entered") && !is.na(df$chn[j - 1])) {
          df$chn[j] <- df$chn[j - 1]
        }
      }
    }
    
    panel_list[[i]] <- df
  }
  return(panel_list)
}

#' 연속된 동일 채널 시청 기록 및 의미 없는 중간 로그(Playback 등)를 처리하는 함수
#' @param panel_list 채널 보정이 완료된 리스트
#' @return 동일 채널 병합이 완료된 리스트
merge_continuous_viewing <- function(panel_list) {
  for (i in seq_along(panel_list)) {
    # 1. 연속된 동일 채널 시청기록 제거 (멤버 활동 로그 제외)
    panel_list[[i]] <- panel_list[[i]] %>%
      mutate(class1 = (chn == lag(chn) & !is.na(chn) & !is.na(lag(chn)) & !startsWith(act, "member"))) %>%
      filter(!class1)
    
    # 2. 동일 채널 시청 중 Playback 등 실질적 시청이 아닌 로그가 있는 경우 처리
    panel_list[[i]] <- panel_list[[i]] %>%
      mutate(
        class2 = (endsWith(act, "on") | endsWith(act, "off") | startsWith(act, "member")),
        class3 = (lag(chn) %in% c(NA, 0, 65535) & !lag(class2) & 
                              chn == lag(chn, n = 2) & time == lag(time))
      ) %>%
      mutate(class3 = if_else(is.na(class3), FALSE, class3)) %>%
      filter(!(class3 | lead(class3, default = FALSE))) %>%
      select(-class1, -class2, -class3)
  }
  return(panel_list)
}


# 1. 멤버 입장 채널 보정 및 무효 채널 처리
met_step6_pre <- entered_chn(met_step5)

# 2. 동일 채널 병합 및 로그 정제 실행
met_step6 <- merge_continuous_viewing(met_step6_pre)


#-------------------------------------------------------------------------------
# 7. 시청 구간 정의 및 시청 로그 정제
#-------------------------------------------------------------------------------

#' 1. 시청 시작(start) 및 종료(end) 시간 생성 함수
#' 다음 로그의 시작 시간에서 1초를 뺀 값을 현재 채널의 종료 시간으로 설정함 
#' @param panel_list 6단계 전처리가 완료된 리스트 (met_step6)
#' @return 시간 구간이 정의된 리스트
define_start_end <- function(panel_list) {
  for (i in seq_along(panel_list)) {
    panel_list[[i]] <- panel_list[[i]] %>%
      mutate(
        start = time,
        time = hms(time),
        # 다음 행의 시간에서 1초를 빼서 종료 시간(end) 생성 
        end = lead(time) - seconds(1),
        end = if_else(is.na(end), NA_character_, 
                      sprintf("%02d:%02d:%02d", 
                              as.integer(end) %/% 3600, 
                              (as.integer(end) %% 3600) %/% 60, 
                              as.integer(end) %% 60))
      ) %>%
      select(-time)
  }
  return(panel_list)
}

#' 2. 비실시간 시청(Playback) 로그 제거 함수
#' VOD 또는 채널 인식 불가 시 발생하는 Playback 로그를 제거함 [cite: 85, 281]
#' @param panel_list 시간 구간이 정의된 리스트
#' @return Playback 로그가 제거된 리스트
filter_playback <- function(panel_list) {
  output_list <- list()
  for (i in seq_along(panel_list)) {
    df <- panel_list[[i]]
    # "Playback" 문자열을 포함하는 행 제외 
    cleaned_df <- df[!grepl("Playback", df$act), ]
    output_list[[names(panel_list)[i]]] <- cleaned_df
  }
  return(output_list)
}

#' 3. 가구원 부재 구간 제거 함수
#' 멤버의 입장/퇴장 로그를 추적하여 실제 시청 가구원이 없는 구간을 제거함 [cite: 101, 189]
#' @param panel_list Playback이 제거된 리스트
#' @return 실시간 시청 인원이 존재하는 로그 리스트
filter_no_member <- function(panel_list) {
  for (i in seq_along(panel_list)) {
    df <- panel_list[[i]]
    member_list <- c()
    member_tracking <- character(nrow(df))
    
    for (j in seq_len(nrow(df))) {
      action_log <- df$act[j]
      
      # 멤버 입장 시 시청 인원 리스트에 추가
      if (grepl("^member \\d+ has entered$", action_log)) {
        m_id <- sub("member (\\d+) has entered", "\\1", action_log)
        member_list <- union(member_list, m_id)
      }
      
      # 멤버 퇴장 시 시청 인원 리스트에서 제거
      if (grepl("^member \\d+ has exited$", action_log)) {
        m_id <- sub("member (\\d+) has exited", "\\1", action_log)
        member_list <- setdiff(member_list, m_id)
      }
      
      member_tracking[j] <- paste(member_list, collapse = "")
    }
    
    df$active_members <- member_tracking
    # 시청 중인 가구원이 없는 구간은 제거함
    df <- df[df$active_members != "", ]
    df <- df %>% select(-active_members)
    panel_list[[i]] <- df
  }
  return(panel_list)
}

#' 4. 유효하지 않은 채널(NA) 로그 제거 함수
#' 전처리 과정에서 NA로 변환된 플랫폼 정보 및 무효 로그를 최종 제거함 [cite: 179]
#' @param panel_list 가구원 필터링이 완료된 리스트
#' @return 최종 유효 시청 로그 리스트
remove_na_chn <- function(panel_list) {
  output_list <- list()
  for (i in seq_along(panel_list)) {
    df <- panel_list[[i]]
    cleaned_df <- df[!is.na(df$chn), ]
    output_list[[names(panel_list)[i]]] <- cleaned_df
  }
  return(output_list)
}


# 각 가공 단계를 순차적으로 적용하여 데이터 정제 수행
met_step7 <- define_start_end(met_step6) %>%
  filter_playback() %>%
  filter_no_member() %>%
  remove_na_chn()

#-------------------------------------------------------------------------------
# 8. 시간 범위 이상치 제거 (Removal of Invalid Time Ranges)
#-------------------------------------------------------------------------------

#' 시청 시작 시간이 종료 시간보다 늦게 기록된 논리적 이상치 행을 제거하는 함수
#' @param panel_list 7단계 정제가 완료된 리스트 (met_step7)
#' @return 시간 논리가 유효한 로그만 남은 리스트 (met_step8)
remove_invalid_time <- function(panel_list) {
  output_list <- list()
  
  for (i in seq_along(panel_list)) {
    df <- panel_list[[i]]
    
    # start와 end를 비교 가능한 시간 형식(hms)으로 임시 변환하여 검증
    valid_df <- df %>%
      mutate(
        tmp_start = hms(start),
        tmp_end = hms(end)
      ) %>%
      # 시작 시간이 종료 시간보다 빠르거나 같은 행만 유지
      filter(is.na(tmp_end) | is.na(tmp_start) | (tmp_end >= tmp_start)) %>%
      select(-tmp_start, -tmp_end)
    
    output_list[[names(panel_list)[i]]] <- valid_df
  }
  
  return(output_list)
}


met_step8 <- remove_invalid_time(met_step7)


#-------------------------------------------------------------------------------
# 9. 시간 형식의 수치화 변환
#-------------------------------------------------------------------------------

#' HH:MM:SS 시간 문자열을 계산 가능한 초 단위 수치로 변환하는 함수
#' @param panel_list 8단계 이상치 제거가 완료된 리스트 (met_step8)
#' @return 초 단위 수치 컬럼(start_unix, end_unix)이 추가된 리스트 (met_step9)
add_unix_time <- function(panel_list) {

  output_list <- lapply(panel_list, function(df) {
    df %>%
      mutate(
        start_unix = as.numeric(hms(start)),
        end_unix   = as.numeric(hms(end))
      )
  })
  return(output_list)
}


met_step9 <- add_unix_time(met_step8)


#-------------------------------------------------------------------------------
# 10. Constant Viewing 1 적용 (12시간 이상 연속 시청 배제)
#-------------------------------------------------------------------------------

#' 동일 채널을 12시간 이상 연속 시청한 기록을 실제 시청이 아닌 것으로 간주하여 제거하는 함수
#' @param panel_list 9단계 수치 변환이 완료된 리스트 (met_step9)
#' @param threshold 기준 시간 (기본값 12시간)
#' @return Constant Viewing 1 규칙이 적용된 리스트 (met_step10)
constant_viewing_1 <- function(panel_list, threshold = 12) {
  
  # 기준 시간을 초 단위로 변환
  threshold_s <- as.numeric(hours(threshold))
  
  output_list <- lapply(panel_list, function(df) {
    df %>%
      mutate(
        gap = end_unix - start_unix
      ) %>%
      # 지속 시간이 기준 시간 미만인 유효 기록만 유지
      filter(gap < threshold_s)
  })
  return(output_list)
}

# Constant Viewing 1 편집 규칙 적용
met_step10 <- constant_viewing_1(met_step9)


#-------------------------------------------------------------------------------
# 11. Constant Viewing 2 적용 (새벽 시간대 고정 시청 배제)
#-------------------------------------------------------------------------------

library(dplyr)
library(lubridate)

#' 새벽 시간대 고정 시청 기록을 편집 규칙에 따라 처리하는 통합 함수
#' @param panel_list 10단계 결과 리스트 (met_step10)
#' @param start_hour 제외 시작 시간 (시 단위, 기본값 2)
#' @param end_hour 제외 종료 시간 (시 단위, 기본값 6)
#' @return Constant Viewing 2 규칙이 적용된 리스트 (met_step11)
constant_viewing_2 <- function(panel_list, start_hour = 2, end_hour = 6) {
  
  # 입력받은 시(hour) 단위를 초 단위로 변환
  start_reject <- start_hour * 3600
  end_reject   <- end_hour * 3600
  
  output_list <- list()
  
  for (i in seq_along(panel_list)) {
    df <- panel_list[[i]]
    panel_id <- names(panel_list)[i]
    
    if (nrow(df) == 0) {
      output_list[[panel_id]] <- df
      next
    }
    
    result <- list()
    
    for (j in seq_len(nrow(df))) {
      start <- df$start_unix[j]
      end   <- df$end_unix[j]
      
      # 1. 구간이 start_reject 이전에 시작하고 end_reject 이후에 끝나는 경우
      if (start <= start_reject & end >= end_reject) {
        # 첫 번째 구간: start_reject 이전
        new_row <- df[j, ]
        new_row$end_unix <- start_reject
        new_row$end <- format(as.POSIXct(new_row$end_unix, origin = "1970-01-01", tz = "UTC"), "%H:%M:%S")
        new_row$gap <- new_row$end_unix - new_row$start_unix
        result[[length(result) + 1]] <- new_row
        
        # 두 번째 구간: end_reject 이후
        new_row2 <- df[j, ]
        new_row2$start_unix <- end_reject
        new_row2$start <- format(as.POSIXct(new_row2$start_unix, origin = "1970-01-01", tz = "UTC"), "%H:%M:%S")
        new_row2$gap <- new_row2$end_unix - new_row2$start_unix
        result[[length(result) + 1]] <- new_row2
      } 
      # 2. start_reject ~ end_reject 사이에 포함되는 경우 그대로 유지
      else if (start > start_reject & end < end_reject) {
        result[[length(result) + 1]] <- df[j, ]
      } 
      # 3. start가 start_reject 이전 시작이고 end가 end_reject 이전에 끝나는 경우
      else if (start <= start_reject & end < end_reject) {
        result[[length(result) + 1]] <- df[j, ]
      }
      # 4. start가 start_reject 이후 시작이고 end가 end_reject 이후에 끝나는 경우
      else if (start > start_reject & end >= start_reject) {
        result[[length(result) + 1]] <- df[j, ]
      } 
      # 5. start와 end가 모두 end_reject 이후인 경우
      else if (start >= end_reject & end >= end_reject) {
        result[[length(result) + 1]] <- df[j, ]
      }
      # 6. start와 end가 모두 start_reject 이전인 경우
      else if (start <= start_reject & end <= start_reject) {
        result[[length(result) + 1]] <- df[j, ]
      } 
      # 7. 기타: 경계 조건을 초과하지 않으면 그대로 유지
      else {
        result[[length(result) + 1]] <- df[j, ]
      }
    }
    
    # 가공된 행들을 합치고 gap > 0만 필터링
    if (length(result) > 0) {
      result_df <- bind_rows(result)
      result_df <- result_df[result_df$gap > 0, ]
      output_list[[panel_id]] <- result_df
    } else {
      output_list[[panel_id]] <- df[0, ]
    }
    
    output_list[[panel_id]] <- output_list[[panel_id]] %>%
      select(-start_unix, -end_unix, -gap, -act)
  }
  return(output_list)
}

met_step11 <- constant_viewing_2(met_step10, start_hour = 2, end_hour = 6)


#-------------------------------------------------------------------------------
# 12. 데이터 통합 및 정형화 (Data Integration & Master Table Creation)
#-------------------------------------------------------------------------------

met_df <- bind_rows(met_step11)
met_df <- met_df %>%
  select(id, chn, start, end, date) %>%
  mutate(
    start = gsub(":", "", start),
    end = gsub(":", "", end),
    date = as.character(date)
  )
