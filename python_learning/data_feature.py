# 函数本身：数据切片统计
# 明细:按照only_mark列group by 计算cols(多个数值字段)的描述性统计字段，years为筛选data时间范围
# 来源：https://mp.weixin.qq.com/s/2TKW-faivNyjMtkbOTBZyw
# merge_ori 暂时还没搞清楚这个参数是干啥的

import pandas as pd

def data_feature(data,cols:list,years:list,only_mark,merge_ori=True):
    df = pd.DataFrame({only_mark:list(set(data[only_mark]))})
    for year in years:
        df1 = pd.DataFrame({only_mark:list(set(data[only_mark]))})
        for col in cols:
            agg_dict = {
                        "last_%s_%s_count"%(year,col):"count",
                        "last_%s_%s_sum"%(year,col):"sum",
                        "last_%s_%s_max"%(year,col):"max",
                        "last_%s_%s_min"%(year,col):"min",
                        "last_%s_%s_mean"%(year,col):"mean",
                        "last_%s_%s_var"%(year,col):"var",
                        "last_%s_%s_std"%(year,col):"std",
                        "last_%s_%s_median"%(year,col):"median",
                        "last_%s_%s_skew"%(year,col):"skew"
                                                                }
            sta_data = data[data.year_diff<=year].groupby([only_mark])[col].agg(agg_dict).reset_index()
            df1 = df1.merge(sta_data,how = "left",on = only_mark)
        df = df.merge(df1,how = "left",on = only_mark)
    if merge_ori:
        df = df.merge(data,how="right",on=only_mark).fillna(0)
    else:
        df = df.fillna(0)
    return  df