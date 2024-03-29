-- stream multi
CREATE OR REPLACE STREAM ACCOUNT_CREDIT_INT_MULTIVALUE WITH (KAFKA_TOPIC='ACCOUNT_CREDIT_INT_MULTIVALUE', PARTITIONS=3) AS SELECT
  DATA.ROWKEY ROWKEY,
  DATA.LOOKUP_KEY LOOKUP_KEY,
  DATA.RECID RECID,
  DATA.OP_TS OP_TS,
  DATA.CURRENT_TS REP_TS,
  TIMESTAMPTOSTRING(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.SSSSSS') CURRENT_TS,
  DATA.TABLE_NAME TABLE_NAME,
  DATA.COMMIT_SCN COMMIT_SCN,
  DATA.COMMIT_ACTION COMMIT_ACTION,
  DATA.XMLRECORD['$FS_multivalue'] $FS,---neu co string join/[$number]
  DATA.XMLRECORD['$FS'] $FS,--- neu k co string join
    ....
  PARSE_T24_MULTIVAL(DATA.RECID, DATA.XMLRECORD, 'ACCOUNT_CREDIT_INT_MULTIVALUE', ARRAY['$FM1,.$FM1...'], ARRAY['$FMS'], '#') XMLRECORD --chọn delimiter nào ko xuất hiện trong value gốc VD:#
FROM FBNK_COLLATERAL_MAPPED DATA
EMIT CHANGES;
 
sửa MV->VM
them VS
 
 
-- stream sink
 
CREATE OR REPLACE STREAM DW_ WITH (PARTITIONS=3) AS SELECT
  DATA.ROWKEY ROWKEY,
  DATA.LOOKUP_KEY LOOKUP_KEY,
  DATA.RECID RECID,
  DATA.OP_TS OP_TS,
  DATA.REP_TS REP_TS,
  TIMESTAMPTOSTRING(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.SSSSSS') CURRENT_TS,
  PARSE_TIMESTAMP(DATA.OP_TS, 'yyyy-MM-dd HH:mm:ss.SSSSSS') COMMIT_TS,
  PARSE_TIMESTAMP(DATA.REP_TS, 'yyyy-MM-dd HH:mm:ss.SSSSSS') REPLICAT_TS,
  PARSE_TIMESTAMP(TIMESTAMPTOSTRING(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.SSSSSS'), 'yyyy-MM-dd HH:mm:ss.SSSSSS') STREAM_TS,
  DATA.TABLE_NAME TABLE_NAME,
  CAST(DATA.COMMIT_SCN AS BIGINT) COMMIT_SCN,
  DATA.COMMIT_ACTION COMMIT_ACTION,
  CAST(CR_MINIMUM_BAL AS double) AS CR_MINIMUM_BAL,,
  ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XML_MV['CR_BASIC_RATE'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),'*') AS ACCOUNT_TITLE_1,
  CAST(CR_OFFSET_ONLY AS double) AS CR_OFFSET_ONLY,,
  XML_MV['CR_BASIC_RATE'] AS CR_BASIC_RATE,,
  XML_MV['CR_MARGIN_OPER'] AS CR_MARGIN_OPER,,
  XML_MV['CR_MAX_RATE'] AS CR_MAX_RATE,,
  XML_MV['CR_MARGIN_RATE'] AS CR_MARGIN_RATE,,
  XML_MV['CR_LIMIT_AMT'] AS CR_LIMIT_AMT,,
  XML_MV['CR_MIN_RATE'] AS CR_MIN_RATE,
  CAST(NVL(DATA.XMLRECORD['IDX'], '1') AS INTEGER) V_M,
  CAST(NVL(DATA.XMLRECORD['IDX_S'], '1') AS INTEGER) V_S,
  (CASE WHEN ((DATA.XMLRECORD['RECID'] = (DATA.RECID + '_TOMBSTONE')) OR (DATA.COMMIT_ACTION = 'D')) THEN 'D' ELSE 'LIVE' END) FLAG_STATUS,
  (CASE WHEN ((SCP.IS_COB_COMPLETED = true) AND (CAST(DATA.COMMIT_SCN AS BIGINT) > SCP.COMMIT_SCN)) THEN PARSE_DATE(SCP.TODAY, 'yyyyMMdd') ELSE PARSE_DATE(SCP.LAST_WORKING_DAY, 'yyyyMMdd') END) BANKING_DATE
FROM ACCOUNT_CREDIT_INT_MULTIVALUE DATA
INNER JOIN FBNK_SEAB_COB_PROCESS_HIGH SCP ON ((SCP.ROWKEY = DATA.LOOKUP_KEY)) --Team SB kiểm tra lại topic cần join --
PARTITION BY DATA.ROWKEY
EMIT CHANGES;