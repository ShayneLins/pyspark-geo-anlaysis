df_region_7 = df_region_cal.filter(col('type')==7).select('type', 'g8')
df_region_6 = df_region_cal.filter(col('type')==6).select('type', 'g7')
df_region_5 = df_region_cal.filter(col('type')==5).select('type', 'g7')
df_region_1 = df_region_cal.filter(col('type')==1).select('type', 'g6')
df_region_2 = df_region_cal.filter(col('type')==2).select('type', 'g6')
df_region_3 = df_region_cal.filter(col('type')==3).select('type', 'g6')
df_region_4 = df_region_cal.filter(col('type')==4).select('type', 'g6')
# type 7
df_ppl_region = df_ppl_cal.join(broadcast(df_region_7), df_ppl_cal.geohash8 == df_region_7.g8, 'left')
df_ppl_region_7 = df_ppl_region.filter(col('type').isNotNull()).select('imei_id', 'agent_id', 'date', 'ts', 'type')
df_ppl_null_region = df_ppl_region.filter(col('type').isNull()).select('imei_id', 'agent_id', 'date', 'ts', 'geohash6', 'geohash7')
# type 6
df_ppl_region = df_ppl_null_region.join(broadcast(df_region_6), df_ppl_null_region.geohash7 == df_region_6.g7, 'left')
df_ppl_region_6 = df_ppl_region.filter(col('type').isNotNull()).select('imei_id', 'agent_id', 'date', 'ts', 'type')
df_ppl_null_region = df_ppl_region.filter(col('type').isNull()).drop('type').drop('g7')
# type 5
df_ppl_region = df_ppl_null_region.join(broadcast(df_region_5), df_ppl_null_region.geohash7 == df_region_5.g7, 'left')
df_ppl_region_5 = df_ppl_region.filter(col('type').isNotNull()).select('imei_id', 'agent_id', 'date', 'ts', 'type')
df_ppl_null_region = df_ppl_region.filter(col('type').isNull()).drop('type').drop('g7')
# type 4
df_ppl_region = df_ppl_null_region.join(broadcast(df_region_4), df_ppl_null_region.geohash6 == df_region_4.g6, 'left')
df_ppl_region_4 = df_ppl_region.filter(col('type').isNotNull()).select('imei_id', 'agent_id', 'date', 'ts', 'type')
df_ppl_null_region = df_ppl_region.filter(col('type').isNull()).drop('type').drop('g6')
# type 3
df_ppl_region = df_ppl_null_region.join(broadcast(df_region_3), df_ppl_null_region.geohash6 == df_region_3.g6, 'left')
df_ppl_region_3 = df_ppl_region.filter(col('type').isNotNull()).select('imei_id', 'agent_id', 'date', 'ts', 'type')
df_ppl_null_region = df_ppl_region.filter(col('type').isNull()).drop('type').drop('g6')
# type 2
df_ppl_region = df_ppl_null_region.join(broadcast(df_region_2), df_ppl_null_region.geohash6 == df_region_2.g6, 'left')
df_ppl_region_2 = df_ppl_region.filter(col('type').isNotNull()).select('imei_id', 'agent_id', 'date', 'ts', 'type')
df_ppl_null_region = df_ppl_region.filter(col('type').isNull()).drop('type').drop('g6')
# type 1
df_ppl_region = df_ppl_null_region.join(broadcast(df_region_1), df_ppl_null_region.geohash6 == df_region_1.g6, 'left')
df_ppl_region_1 = df_ppl_region.filter(col('type').isNotNull()).select('imei_id', 'agent_id', 'date', 'ts', 'type')
# type 8
df_ppl_region_8 = df_ppl_region.na.fill(8, ['type']).select('imei_id', 'agent_id', 'date', 'ts', 'type')

df_ppl_region = df_ppl_region_7.union(df_ppl_region_6).union(df_ppl_region_5).union(df_ppl_region_4).union(df_ppl_region_3).union(df_ppl_region_2).union(df_ppl_region_1).union(df_ppl_region_8)