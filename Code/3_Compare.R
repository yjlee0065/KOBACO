library(dplyr)
library(purrr)

# -------------------------------------------------------------------------------
# 1. Uncovered Viewing 리젝된 가구 비교
# -------------------------------------------------------------------------------

# 원시 데이터 가구 ID
original_ids <- names(met_step1)
# 연구실 규칙 적용 완료 데이터 가구 ID
met_final_ids <- as.character(unique(met_df$id))
# 닐슨 규칙 적용 완료 데이터 가구 ID
tx3_final_ids <- as.character(unique(tx3$id))

# 연구실(MET)에서 리젝된 가구
met_reject_ids <- setdiff(original_ids, met_final_ids)
# 닐슨(TX3)에서 리젝된 가구
tx3_reject_ids <- setdiff(original_ids, tx3_final_ids)

reject_summary <- data.frame(
  DF_Reject      = length(met_reject_ids),   # MET 리젝
  TX3_Reject     = length(tx3_reject_ids),   # TX3 리젝
  # 연구실측과 닐슨측 모두에서 제외된 가구
  Both_Reject    = length(intersect(met_reject_ids, tx3_reject_ids)),
  # MET에서만 리젝
  DF_OnlyReject  = length(setdiff(met_reject_ids, tx3_reject_ids)),
  # TX3에서만 리젝
  TX3_OnlyReject = length(setdiff(tx3_reject_ids, met_reject_ids)),
  stringsAsFactors = FALSE
)

reject_summary


# -------------------------------------------------------------------------------
# 2. 개별 시청기록 비교
# -------------------------------------------------------------------------------

# 리젝되지 않은 공통 가구 추출
common_ids <- intersect(unique(met_df$id), unique(tx3$id))

met_compare <- met_df %>% 
  filter(id %in% common_ids) %>%
  mutate(across(c(id, chn, start, end, date), as.character))

tx3_compare <- tx3 %>% 
  filter(id %in% common_ids) %>%
  mutate(across(c(id, chn, start, end, date), as.character))

# 분석 대상 날짜 추출
all_dates <- unique(c(met_compare$date, tx3_compare$date))

# 날짜별 순차 필터링 및 집계
summary_by_date <- map_df(all_dates, function(d) {
  
  # 해당 날짜 데이터 필터링
  m_sub <- met_compare %>% filter(date == d)
  t_sub <- tx3_compare %>% filter(date == d)
  
  m_total <- nrow(m_sub)
  t_total <- nrow(t_sub)
  
  # 1) 완벽 매칭 (Perfect Match)
  perf <- inner_join(m_sub, t_sub, by = c("id", "chn", "start", "end", "date"))
  m_rem <- anti_join(m_sub, perf, by = c("id", "chn", "start", "end", "date"))
  t_rem <- anti_join(t_sub, perf, by = c("id", "chn", "start", "end", "date"))
  
  # 2) 시작 시각 불일치 (ID, Chn, End, Date 일치)
  start_mis <- inner_join(m_rem, t_rem, by = c("id", "chn", "end", "date"), suffix = c("_m", "_t")) %>%
    distinct(id, chn, end, date, .keep_all = TRUE)
  m_rem <- anti_join(m_rem, start_mis, by = c("id", "chn", "end", "date"))
  t_rem <- anti_join(t_rem, start_mis, by = c("id", "chn", "end", "date"))
  
  # 3) 종료 시각 불일치 (ID, Chn, Start, Date 일치)
  end_mis <- inner_join(m_rem, t_rem, by = c("id", "chn", "start", "date"), suffix = c("_m", "_t")) %>%
    distinct(id, chn, start, date, .keep_all = TRUE)
  m_rem <- anti_join(m_rem, end_mis, by = c("id", "chn", "start", "date"))
  t_rem <- anti_join(t_rem, end_mis, by = c("id", "chn", "start", "date"))
  
  # 4) 채널 번호 불일치 (ID, Start, End, Date 일치)
  chn_mis <- inner_join(m_rem, t_rem, by = c("id", "start", "end", "date"), suffix = c("_m", "_t")) %>%
    distinct(id, start, end, date, .keep_all = TRUE)
  m_rem <- anti_join(m_rem, chn_mis, by = c("id", "start", "end", "date"))
  t_rem <- anti_join(t_rem, chn_mis, by = c("id", "start", "end", "date"))
  
  # 결과 반환
  data.frame(
    date = d,
    Matched = nrow(perf),
    Start_Mismatched = nrow(start_mis),
    End_Mismatched = nrow(end_mis),
    Chn_Mismatched = nrow(chn_mis),
    Lab_Only = nrow(m_rem),
    TX3_Only = nrow(t_rem),
    Lab_Total = m_total,
    TX3_total = t_total,
    stringsAsFactors = FALSE
  )
})

summary_by_date