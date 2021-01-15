from pyecharts import options as opts
from pyecharts.charts import Map, Timeline
import geopandas as gp
import pandas as pd
import datetime

# 此函数将原始数据抽取，抽取出国内疫情数据，并按照日期省份分组，做nan =0填充
def oringinal_data(args):
    print("开始抽取中国疫情数据，请等待......")
    data = pd.DataFrame(pd.read_csv(args, encoding="gbk"))
    province_list = ["河北", "山西", "辽宁", "吉林", "黑龙江",
                     "江苏", "浙江", "安徽", "福建", "江西",
                     "山东", "河南", "湖北", "湖南", "广东",
                     "海南", "四川", "贵州", "云南", "陕西",
                     "甘肃", "青海", "台湾", "广西", "新疆",
                     "内蒙古", "宁夏", "西藏", "北京", "天津",
                     "上海", "重庆", "香港", "澳门"]
    df = pd.DataFrame()
    for index, row in data.iterrows():
        if row["province"] in province_list:
            df = df.append(row)
    df[["inflect", "cure", "dead"]] = data[["inflect", "cure", "dead"]].fillna(0)
    del df["city"]
    df1 = df.groupby(["date", "province"], as_index=False).agg({"inflect": "sum", "cure": "sum", "dead": "sum", })
    print("中国省份数据抽取填充完成")
    return df1

# 进行地图数据绑定扩充，保证每天每个省份均有数据
def standard(data):
    print("正在进行数据扩张，进行规范化处理，请等待......")
    data.rename(columns={'province': 'NAME'}, inplace=True)
    china_geod = gp.GeoDataFrame.from_file('province')  # 读取shp文件
    # 如果有省份不在地图的NAME中，则打印出错信息，为了地图绑定数据做准备
    for index, row in data.iterrows():
        if row["NAME"] not in china_geod["NAME"].tolist():
            print(row)
        else:
            pass
    # 查询日期列表
    time_list = data["date"]
    new_time_list = list(set(time_list))
    # 进行数据拼接，数据按天绑定
    demo_df = pd.DataFrame()
    for day in new_time_list:
        tmp = pd.DataFrame()
        for index, row in data.iterrows():
            if row.date == day:
                tmp = tmp.append(row)
        # 将数据扩张，保证每一天各个省份均有数据，空值使用0元素填充，并且生成新的数据文件
        merge = pd.merge(china_geod, tmp, on="NAME", how="outer")
        merge["date"] = day
        merge.drop(merge.columns[0:119], axis=1, inplace=True)
        merge.drop(merge.columns[1], axis=1, inplace=True)
        merge[["inflect", "cure", "dead"]] = merge[["inflect", "cure", "dead"]].fillna(0)
        demo_df = demo_df.append(merge, ignore_index=True)
        for index, row in demo_df.iterrows():
            str = row["date"]
            m = str.split("月")[0]
            d = str.split("月")[1].replace("日", "")
            y = "2020-"
            date = y + m + "-" + d
            row.date = datetime.datetime.strptime(date, "%Y-%m-%d")
        print(day + "数据扩张处理已完成")
    demo_df.sort_values(by="date", inplace=True)
    demo_df.groupby(by="date", as_index=True)
    # demo_df.drop(data.columns[0:1], axis=1, inplace=True)
    data.reindex()
    print("数据扩张和规范化处理已结束！")
    demo_df.to_csv("test.csv", encoding="GBK")
    return demo_df

def date_time():
    date_list = []
    begin_date = datetime.datetime.strptime("2020.1.11", "%Y.%m.%d")
    end_date = datetime.datetime.strptime("2020.7.31", "%Y.%m.%d")
    while begin_date <= end_date:
        date_tmp = str(begin_date.month) + "月" + str(begin_date.day) + "日"
        date_list.append(date_tmp)
        begin_date += datetime.timedelta(days=1)
    return date_list

# 画图
def rend_map(data):
    print("开始绘图")
    tl = Timeline(
        init_opts=opts.InitOpts(width="1300px", height="750px", page_title="COVID-19新冠肺炎疫情中国趋势")
    )
    date_list = date_time()
    # 累计感染，累计治愈，# 累计死亡
    sum_inflect = 0
    sum_cure = 0
    sum_dead = 0
    str_tmp = ""
    for date_item in date_list:
        data3 = pd.DataFrame()
        for index, row in data.iterrows():
            if row.date == date_item:
                data3 = data3.append(row)
        if data3.empty == True:
            print(date_item + "为空数据")
            pass
        else:
            sum_inflect = sum_inflect + data3.inflect.sum()
            sum_dead = sum_dead + data3.dead.sum()
            sum_cure = sum_cure + data3.cure.sum()
            str_tmp = "感染人数:" + str(sum_inflect).split(".")[0] + " 治愈人数: " + str(sum_cure).split(".")[0] + " 死亡人数: " + \
                      str(sum_dead).split(".")[0]
            print(date_item + "正在绘图")
            map0 = (
                Map()
                    .add(
                    str_tmp,
                    [list(z) for z in zip(data3["NAME"], data3["inflect"])],
                    "china",
                    zoom=1.0
                )
                    .set_series_opts(label_opts=opts.LabelOpts(is_show=True))  # 显示省份名
                    .set_global_opts(
                    title_opts=opts.TitleOpts(
                        title="COVID-19中国每日新增地图({})".format("2020年" + date_item),
                        subtitle="截至到2020年7月31日",
                        pos_left="left",
                        pos_top='top',
                        title_textstyle_opts=opts.TextStyleOpts(
                            font_size=22,
                            color="#ff0000"
                        )
                    ),
                    visualmap_opts=opts.VisualMapOpts(
                        is_piecewise=True,
                        pieces=[
                            {"min": 10000, "label": '>10000人', "color": "#000000"},  # 不指定 max，表示 max 为无限大（Infinity）。
                            {"min": 1000, "max": 9999, "label": '1000-9999人', "color": "#4d0000"},
                            {"min": 500, "max": 999, "label": '500-999人', "color": "#990000"},
                            {"min": 100, "max": 499, "label": '100-499人', "color": "#e60000"},
                            {"min": 10, "max": 99, "label": '10-99人', "color": "#ff3333"},
                            {"min": 1, "max": 9, "label": '1-9人', "color": "#ff8080"},
                            {"value": 0, "label": '0人', "color": ' #f2f2f2'},
                        ]
                    )
                )

            )
        tl.add(map0, "{}".format(date_item))
    tl.add_schema(is_auto_play=True, play_interval=500, is_loop_play=False)
    tl.render("中国疫情地图.html")
    print("绘图结束！")

if __name__ == '__main__':
    # args = "yssj.csv"
    # data = oringinal_data(args)
    # data2 = standard(data)
    data2=pd.DataFrame(pd.read_csv("test.csv", encoding="gbk"))
    rend_map(data2)
    print("执行结束！")
