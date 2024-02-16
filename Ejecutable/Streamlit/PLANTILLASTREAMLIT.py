# streamlit
import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px

def load_data():
    df = pd.read_csv('eventos_db.csv')        #SUSTITUIR POR LOS NUESTROS
    return df

def main():
    st.title('Panel de Análisis de Eventos')

    df = load_data()

    # Bar chart of Profits by Event
    st.subheader('Ganancias por Evento')
    pivot_table = pd.pivot_table(df, index='nombre_ev', values='diferencia', aggfunc='sum', sort='ASC')
    pivot_table = pivot_table.sort_values(by='diferencia', ascending=False)
    st.bar_chart(pivot_table['diferencia'])

    # Key Performance Indicators (KPIs)
    total_difference = round(df['diferencia'].sum(), 2)
    unique_events = df['nombre_ev'].nunique()
    average_age = round(df['edad'].mean(), 0)
    total_events = df.shape[0]

    st.subheader('Indicadores Clave de Desempeño (KPIs)')
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label='Ganancias Totales', value=total_difference, delta=None)
    with col2:
        st.metric(label='Eventos Únicos', value=unique_events, delta=None)
    with col3:
        st.metric(label='Edad Promedio', value=average_age, delta=None)
    with col4:
        st.metric(label='Total de Eventos', value=total_events, delta=None)

    # Interactive Scatter Plot
    st.subheader('Gráfico de dispersión de Ganancias por Edad')
    scatter_fig = px.scatter(df, x='edad', y='diferencia_porcentaje', color='tipo_ev',
                             size='importe_activos_0', hover_data=['nombre_ev'],
                             title='Gráfico de dispersión de Ganancias por Edad')
    scatter_fig.update_layout(xaxis={'title':'Edad'}, yaxis={'title':'Diferencia porcentaje de Ganancias'})
    st.plotly_chart(scatter_fig)

    # Analysis by Year
    df['año'] = pd.to_datetime(df['fecha_ev']).dt.year
    year_profit = df.groupby('año')['diferencia'].sum().reset_index()

    # Bar chart with profits grouped by year
    st.subheader('Análisis por Año')
    bar_chart = px.bar(year_profit, x='año', y='diferencia',
                       title='Ganancias a lo largo de los años', labels={'diferencia':'Ganancias'})
    bar_chart.update_layout(xaxis={'categoryorder':'total descending'})
    st.plotly_chart(bar_chart)

    # Bar chart of Profits by 'prov'
    st.subheader('Ganancias por Provincia')
    province_profit = df.groupby('prov')['diferencia'].sum().reset_index()
    province_chart = px.bar(province_profit, x='prov', y='diferencia',
                            title='Ganancias por Provincia', labels={'diferencia':'Ganancias'})
    province_chart.update_layout(xaxis={'categoryorder':'total descending'})
    st.plotly_chart(province_chart)
    
    # Show raw event data
    st.subheader('Datos Raw del Evento')
    st.write(df)

if __name__ == "__main__":
    main()
