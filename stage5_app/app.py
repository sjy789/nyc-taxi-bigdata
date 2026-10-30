"""NYC Taxi Analytics + One-Hour-Ahead Demand Forecast | Stage 5 Streamlit app.

Data contract: Stage 3 summary CSVs, Stage 4 June Parquet predictions.
This is a HISTORICAL TEST visualization: not live NYC taxi demand or unseen future forecasts.
"""
from __future__ import annotations
import copy
import json
from datetime import date
from pathlib import Path

import math
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

ROOT = Path('/data')
MODELS = {'baseline':'历史同期 Baseline','rf':'随机森林 RF','gbt':'梯度提升树 GBT'}
ORDER_HOURS = list(range(24))

st.set_page_config(page_title='NYC Taxi | 出行大数据系统', page_icon='🚕', layout='wide', initial_sidebar_state='expanded')
st.markdown('''<style>
.block-container {padding-top:1.35rem;padding-bottom:2rem;max-width:1480px}
h1 {color:#123259;font-weight:750}
[data-testid="stMetric"] {background:#f6f9fd;padding:13px;border:1px solid #e6edf7;border-radius:12px}
[data-testid="stSidebar"] {background:#f4f7fb}
</style>''', unsafe_allow_html=True)

@st.cache_data(show_spinner=False)
def load_json(filename):
    return json.loads((ROOT/'reports'/filename).read_text(encoding='utf-8'))

@st.cache_data(show_spinner=False)
def load_table(name):
    paths = sorted((ROOT/'marts'/name).glob('part-*.csv'))
    if not paths:
        raise FileNotFoundError(f'缺少 Stage3 CSV: data/marts/{name}/part-*.csv')
    return pd.concat((pd.read_csv(path) for path in paths), ignore_index=True)

@st.cache_data(show_spinner=False)
def load_predictions():
    path = ROOT/'ml'/'predictions_test'
    df = pd.read_parquet(path, columns=['zone_id','hour_key','date','hour','label','pred_baseline','pred_rf','pred_gbt'])
    df['zone_id'] = df['zone_id'].astype(int)
    df['hour'] = df['hour'].astype(int)
    df['hour_key'] = df['hour_key'].astype(str)
    df['date'] = df['date'].astype(str)
    return df

@st.cache_data(show_spinner=False)
def load_lookup():
    table=pd.read_csv(ROOT/'raw'/'taxi_zone_lookup.csv')
    table['LocationID']=pd.to_numeric(table['LocationID'],errors='coerce')
    return table.dropna(subset=['LocationID']).drop_duplicates('LocationID').set_index('LocationID')

@st.cache_data(show_spinner=False)
def load_geo():
    path = ROOT/'marts'/'taxi_zones_normalized.geojson'
    return json.loads(path.read_text(encoding='utf-8'))

def fmt_number(num):
    return f'{float(num):,.0f}'

def apply_style(fig, height=360):
    fig.update_layout(template='plotly_white', height=height, margin=dict(l=20,r=15,t=45,b=20),
        font=dict(size=13, family='Arial'), legend_title_text='')
    return fig

def zone_label(zone_id, lookup):
    z=int(zone_id)
    if z not in lookup.index:
        return f'{z} · 未命名区域'
    line=lookup.loc[z]
    return f'{z} · {line["Borough"]} / {line["Zone"]}'

try:
    summary3=load_json('stage3_summary.json')
    summary4=load_json('stage4_metrics.json')
    features_meta=load_json('stage4_features_summary.json')
    if summary3['status']!='SUCCESS' or summary4['status']!='SUCCESS':
        raise ValueError('Stage3/Stage4 未成功完成')
    lookup=load_lookup()
except Exception as exc:
    st.error(f'缺少前序阶段输出：{exc}')
    st.stop()

with st.sidebar:
    st.title('🚕 NYC Taxi')
    st.caption('Spark SQL · Spark MLlib · Streamlit')
    page=st.radio('系统功能', ['📊 数据总览','📅 出行规律','🗺️ 区域热力图','🔮 需求预测','📐 模型评估'], index=0)
    st.divider()
    st.caption('分析月份：2024 年 1 月')
    st.caption('预测回测：2024 年 6 月（30 个区域）')
    st.caption('本系统读取离线成果，浏览时无需运行 Spark。')
    st.divider()
    st.caption('数据来源：NYC TLC / NYC Open Data')

if page == '📊 数据总览':
    st.title('纽约出租车出行数据分析与预测系统')
    st.caption('基于 Apache Spark 的真实历史订单统计，以及使用 Spark MLlib 完成的下一小时订单预测回测')
    k=summary3['kpi']
    c=st.columns(5)
    c[0].metric('1 月清洗后订单',fmt_number(k['trip_count']))
    c[1].metric('有订单的上车区域',fmt_number(k['pickup_zone_count']))
    c[2].metric('平均行程距离',f'{float(k["avg_distance_miles"]):.2f} 英里')
    c[3].metric('平均行程时间',f'{float(k["avg_duration_min"]):.2f} 分钟')
    c[4].metric('平均订单金额',f'${float(k["avg_order_amount_usd"]):.2f}')
    daily=load_table('daily_trips').sort_values('date')
    fig=px.line(daily,x='date',y='trip_count',markers=True,
        title='2024 年 1 月每日出租车订单量',labels={'date':'日期','trip_count':'订单量'})
    st.plotly_chart(apply_style(fig,380),use_container_width=True)
    a,b,c=st.columns(3)
    a.metric('1～6 月有效订单',fmt_number(load_json('stage4_prepare_summary.json')['total_valid_rows']))
    b.metric('6 月测试样本',fmt_number(summary4['test_rows']))
    best=summary4['best_model_by_validation_mae']
    c.metric('验证集最佳模型',MODELS.get(best,best))
    st.info('上方统计属于 2024 年 1 月；模型指标属于 2024 年 6 月。二者时间范围不同，不能混为同一月份的数据。')

elif page == '📅 出行规律':
    st.title('2024 年 1 月 · 出行规律')
    daily=load_table('daily_trips').sort_values('date')
    hourly=load_table('hourly_trips').sort_values('hour')
    hourly['hour']=hourly['hour'].astype(int)
    left,right=st.columns(2)
    with left:
        fig=px.line(daily,x='date',y='trip_count',markers=True,title='每日订单趋势',
            labels={'date':'日期','trip_count':'订单量'})
        st.plotly_chart(apply_style(fig),use_container_width=True)
    with right:
        fig=px.bar(hourly,x='hour',y='trip_count',title='24 小时订单分布',
            labels={'hour':'小时','trip_count':'订单量'})
        fig.update_xaxes(dtick=2)
        st.plotly_chart(apply_style(fig),use_container_width=True)
    daily['weekday']=pd.to_datetime(daily['date']).dt.dayofweek
    weekday=daily.groupby('weekday',as_index=False).agg(
        mean_orders=('trip_count','mean'),total_orders=('trip_count','sum'),days=('date','count'))
    weekday['name']=weekday['weekday'].map(dict(enumerate(['周一','周二','周三','周四','周五','周六','周日'])))
    st.subheader('星期分布：按日均订单量比较')
    fig=px.bar(weekday,x='name',y='mean_orders',text='days',
        labels={'name':'星期','mean_orders':'日均订单量','days':'出现天数'})
    fig.update_traces(texttemplate='统计%{text}天',textposition='outside')
    st.plotly_chart(apply_style(fig),use_container_width=True)
    st.caption('各星期在 2024 年 1 月出现的天数不完全相同，故使用日均订单量比较，避免总量偏差。')
    c1,c2=st.columns(2)
    with c1:
        dist=load_table('distance_distribution')
        fig=px.bar(dist,x='distance_bin',y='trip_count',title='行程距离分布（英里）',
            labels={'distance_bin':'距离区间','trip_count':'订单量'})
        st.plotly_chart(apply_style(fig),use_container_width=True)
    with c2:
        amount=load_table('amount_distribution')
        fig=px.bar(amount,x='amount_bin',y='trip_count',title='订单金额分布（美元）',
            labels={'amount_bin':'金额区间','trip_count':'订单量'})
        st.plotly_chart(apply_style(fig),use_container_width=True)

elif page == '🗺️ 区域热力图':
    st.title('2024 年 1 月 · 出租车区域热力图')
    st.caption('以真实历史订单为依据，按纽约出租车分区边界展示某个日期、某个小时的上车订单量。')
    c1,c2,c3=st.columns([2,2,2])
    date_chosen=c1.date_input('日期',value=date(2024,1,16),min_value=date(2024,1,1),max_value=date(2024,1,31))
    hour_chosen=c2.slider('小时（当地时间）',0,23,18)
    scale=c3.selectbox('着色方式',['线性','对数增强'],index=1)
    byzone=load_table('zone_date_hourly_trips')
    mask=(byzone['date'].astype(str)==str(date_chosen)) & (pd.to_numeric(byzone['hour'],errors='coerce')==hour_chosen)
    sub=byzone.loc[mask,['location_id','trip_count']].copy()
    byid={int(row.location_id):int(row.trip_count) for row in sub.itertuples(index=False)}
    st.metric('该时段已记录的上车订单',fmt_number(sum(byid.values())))
    try:
        geo=copy.deepcopy(load_geo())
    except Exception as exc:
        st.error(f'缺少地图分区边界：{exc}。先按阶段五步骤下载并准备官方 GeoJSON。')
        st.stop()
    values=list(byid.values()) or [0]
    vmax=max(values) if max(values)>0 else 1
    for feature in geo['features']:
        props=feature['properties']
        zid=int(props['zone_id'])
        qty=byid.get(zid,0)
        ratio=(qty/vmax if scale=='线性' else (math.log1p(qty)/math.log1p(vmax)))
        props['trip_count']=qty
        props['display_name']=f'{props["borough"]} / {props["zone"]}'
        props['fill_color']=[int(230-202*ratio),int(241-104*ratio),int(252-64*ratio),210]
    layer=pdk.Layer('GeoJsonLayer', data=geo, filled=True,stroked=True,
        get_fill_color='properties.fill_color',get_line_color=[255,255,255,160],
        line_width_min_pixels=0.5,pickable=True,auto_highlight=True)
    chart=pdk.Deck(layers=[layer],initial_view_state=pdk.ViewState(
        latitude=40.731, longitude=-73.944,zoom=10,pitch=0),map_provider=None,
        tooltip={'html':'<b>{properties.display_name}</b><br/>区域 ID：{properties.zone_id}<br/>上车订单：{properties.trip_count}',
                 'style':{'color':'white'}})
    st.pydeck_chart(chart, height=580)
    st.caption('颜色越深表示该区域当小时订单量越高；灰白底图是离线地图模式，无需地图 API Key。')
    toplist=(pd.DataFrame({'zone_id':list(byid.keys()),'trip_count':list(byid.values())})
             .sort_values('trip_count',ascending=False).head(15))
    toplist['区域']=toplist['zone_id'].apply(lambda x:zone_label(x,lookup))
    st.subheader('所选时段的热门区域 Top 15')
    fig=px.bar(toplist.sort_values('trip_count'),y='区域',x='trip_count',orientation='h',
               labels={'trip_count':'订单量'},height=520)
    st.plotly_chart(apply_style(fig,520),use_container_width=True)
    with st.expander('查看 1 月整体热门上车区域与路线'):
        a,b=st.columns(2)
        a.dataframe(load_table('top_pickup_zones'),hide_index=True,use_container_width=True)
        b.dataframe(load_table('top_routes'),hide_index=True,use_container_width=True)

elif page == '🔮 需求预测':
    st.title('2024 年 6 月 · 下一小时订单预测（历史回测）')
    st.caption('选择区域、日期及小时，查看已保存的实际订单量与模型预测值。仅展示 2024 年 6 月测试集，不宣称实时或未来预测。')
    pred=load_predictions()
    zones=sorted(pred['zone_id'].unique())
    # Preserve train-only Top 30 ordering as saved in Stage 4 metadata.
    known=[int(z) for z in features_meta.get('zones',[]) if int(z) in zones]
    zone_options=known or zones
    a,b,c,d=st.columns([3,2,2,2])
    selected_zone=a.selectbox('出租车区域',zone_options,format_func=lambda x:zone_label(x,lookup))
    selected_date=b.date_input('预测日期',value=date(2024,6,13),min_value=date(2024,6,1),max_value=date(2024,6,30))
    selected_hour=c.slider('小时',0,23,18)
    chosen=d.selectbox('预测方法',list(MODELS),index=list(MODELS).index(summary4['best_model_by_validation_mae']),format_func=lambda m:MODELS[m])
    series=pred[(pred.zone_id==selected_zone)&(pred.date==str(selected_date))].sort_values('hour')
    line=series[series.hour==selected_hour]
    if line.empty:
        st.warning('所选时段没有预测数据。')
        st.stop()
    row=line.iloc[0]
    real=float(row['label']); prediction=float(row['pred_'+chosen])
    k1,k2,k3,k4=st.columns(4)
    k1.metric('实际订单数',fmt_number(real))
    k2.metric('模型预测订单数',f'{prediction:,.1f}')
    k3.metric('绝对误差',f'{abs(real-prediction):,.1f}')
    k4.metric('数据日期',str(selected_date))
    st.subheader(f'{zone_label(selected_zone,lookup)}：当日逐小时预测')
    fig=go.Figure()
    fig.add_trace(go.Scatter(x=series.hour,y=series.label,name='实际订单量',mode='lines+markers'))
    fig.add_trace(go.Scatter(x=series.hour,y=series['pred_'+chosen],name=MODELS[chosen]+' 预测',mode='lines+markers'))
    fig.update_layout(xaxis_title='小时',yaxis_title='订单量')
    fig.update_xaxes(dtick=1)
    st.plotly_chart(apply_style(fig,430),use_container_width=True)
    zdata=pred[pred.zone_id==selected_zone].sort_values('hour_key')
    start_day=st.date_input('连续趋势起始日期（显示 7 天）', value=date(2024,6,10),
        min_value=date(2024,6,1),max_value=date(2024,6,24))
    starts=pd.Timestamp(start_day)
    subset=zdata[(pd.to_datetime(zdata['date'])>=starts)&(pd.to_datetime(zdata['date'])<starts+pd.Timedelta(days=7))]
    fig=go.Figure()
    fig.add_trace(go.Scatter(x=subset.hour_key,y=subset.label,name='实际订单量',mode='lines'))
    fig.add_trace(go.Scatter(x=subset.hour_key,y=subset['pred_'+chosen],name=MODELS[chosen]+' 预测',mode='lines'))
    st.plotly_chart(apply_style(fig,380),use_container_width=True)
    st.caption('预测以之前已经发生的小时订单量作为历史特征；所有曲线是 2024 年 6 月独立测试集的离线回测。')
    st.download_button('下载该区域 6 月预测结果 CSV',data=zdata.to_csv(index=False).encode('utf-8-sig'),
        file_name=f'taxi_zone_{selected_zone}_2024_06.csv',mime='text/csv')

elif page == '📐 模型评估':
    st.title('Spark MLlib · 模型效果评估')
    st.caption('2024 年 1～4 月训练 · 5 月验证选择模型 · 6 月独立测试 · 30 个区域')
    train,valid,test=st.columns(3)
    train.metric('训练样本',fmt_number(summary4['train_rows']))
    valid.metric('验证样本',fmt_number(summary4['validation_rows']))
    test.metric('测试样本',fmt_number(summary4['test_rows']))
    chosen=summary4['best_model_by_validation_mae']
    st.success(f'根据 5 月验证集 MAE 选出的最佳模型：{MODELS[chosen]}')
    period=st.radio('评估区间',['2024 年 5 月验证集','2024 年 6 月测试集'],horizontal=True,index=1)
    key='validation' if '验证' in period else 'test'
    records=[{'模型':MODELS[n], 'MAE':summary4[key]['models'][n]['MAE'],
              'RMSE':summary4[key]['models'][n]['RMSE'],
              'WAPE (%)':summary4[key]['models'][n]['WAPE_percent']} for n in MODELS]
    score=pd.DataFrame(records)
    st.dataframe(score,hide_index=True,use_container_width=True)
    col1,col2,col3=st.columns(3)
    for col, metric in zip((col1,col2,col3),('MAE','RMSE','WAPE (%)')):
        fig=px.bar(score,x='模型',y=metric,title=f'{metric}（越低越好）',text_auto='.2f')
        fig.update_xaxes(tickangle=-20)
        col.plotly_chart(apply_style(fig,330),use_container_width=True)
    st.subheader('六月总订单量：实际值与预测值')
    pred=load_predictions()
    aggregated=(pred.groupby('date',as_index=False)[['label','pred_baseline','pred_rf','pred_gbt']].sum()
                .sort_values('date'))
    fig=go.Figure()
    fig.add_trace(go.Scatter(x=aggregated.date,y=aggregated.label,mode='lines+markers',name='真实订单量'))
    fig.add_trace(go.Scatter(x=aggregated.date,y=aggregated['pred_'+chosen],mode='lines+markers',name=MODELS[chosen]+' 预测'))
    st.plotly_chart(apply_style(fig,430),use_container_width=True)
    with st.expander('模型训练时间与配置'):
        st.json({'training_seconds':summary4.get('training_seconds',{}),
                 'parameters':summary4.get('parameters',{}),
                 'evaluation_definition':summary4.get('interpretation','')})
    st.caption('MAE 与 RMSE 的单位是每个区域每小时的订单数。测试指标不用于选择模型。')
