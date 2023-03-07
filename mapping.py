import plotly.express as px
import pandas as pd
import data_analysis


df = data_analysis.prep_data()

fig = px.choropleth(
    df,
    locationmode="USA-states",
    locations='state_abbrev',
    color='n_killed',
    animation_frame="date",
    scope='usa'
)

fig.show()
