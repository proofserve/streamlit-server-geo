import streamlit as st
import pandas as pd
from streamlit_keplergl import keplergl_static
from keplergl import KeplerGl
from google.oauth2 import service_account
from google.cloud import bigquery


st.set_page_config(layout="wide")

col1, col2 = st.columns(2)

server_id = col1.number_input(label="Server ID", value=5041)
lookback_days = col2.number_input(label="Lookback Days", value=90)

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

name_rows = run_query("select name from proof.users where id = %s" % (server_id))
st.write("Showing jobs over last %s days for server %s" % (lookback_days, name_rows[0]['name']))

rows = run_query("""WITH PrimaryAddresses AS (
SELECT
job_addresses.id,
job_addresses.job_id,
job_addresses.zip,
job_addresses.cell,
ROW_NUMBER() OVER (PARTITION BY job_addresses.job_id ORDER BY job_addresses.deleted_at ASC, job_addresses.primary DESC) as rn
FROM
proof.job_addresses
)

select
FORMAT("%%x", PrimaryAddresses.cell) as cell,
count(*) as jobs
from proof.jobs
left join PrimaryAddresses on PrimaryAddresses.job_id = jobs.id and PrimaryAddresses.rn = 1
where server_id = %s
and created_at >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL %s DAY)
group by PrimaryAddresses.cell
""" % (server_id, lookback_days))


df = pd.DataFrame(rows)

map_1 = KeplerGl(height=1000, data={"data": df}, config={
    "version": "v1",
    "config": {
        "visState": {
            "filters": [],
            "layers": [
                {
                    "id": "data",
                    "type": "hexagonId",
                    "config": {
                        "dataId": "data",
                        "label": "Data",
                        "color": [255, 203, 153],
                        "highlightColor": [252, 242, 26, 255],
                        "columns": {"hex_id": "cell"},
                        "isVisible": True,
                        "visConfig": {
                            "colorRange": {
                                "name": "ColorBrewer YlGn-6",
                                "type": "sequential",
                                "category": "ColorBrewer",
                                "colors": ["#ffffcc", "#d9f0a3", "#addd8e", "#78c679", "#31a354", "#006837"],
                            },
                            "filled": True,
                            "opacity": 0.8,
                            "outline": False,
                            "strokeColor": None,
                            "strokeColorRange": {
                                "name": "Global Warming",
                                "type": "sequential",
                                "category": "Uber",
                                "colors": ["#5A1846", "#900C3F", "#C70039", "#E3611C", "#F1920E", "#FFC300"],
                            },
                            "strokeOpacity": 0.8,
                            "thickness": 2,
                            "coverage": 1,
                            "enable3d": True,
                            "sizeRange": [0, 500],
                            "coverageRange": [0, 1],
                            "elevationScale": 5,
                            "enableElevationZoomFactor": True,
                        },
                        "hidden": False,
                        "textLabel": [
                            {
                                "field": None,
                                "color": [255, 255, 255],
                                "size": 18,
                                "offset": [0, 0],
                                "anchor": "middle",
                                "alignment": "center",
                                "outlineWidth": 0,
                                "outlineColor": [255, 0, 0, 255],
                                "background": False,
                                "backgroundColor": [0, 0, 200, 255],
                            }
                        ],
                    },
                    "visualChannels": {
                        "colorField": {"name": "jobs", "type": "integer"},
                        "colorScale": "quantile",
                        "strokeColorField": None,
                        "strokeColorScale": "quantile",
                        "sizeField": {"name": "jobs", "type": "integer"},
                        "sizeScale": "linear",
                        "coverageField": None,
                        "coverageScale": "linear",
                    },
                }
            ],
            "effects": [],
            "interactionConfig": {
                "tooltip": {
                    "fieldsToShow": {
                        "-53x0gu": [
                            {"name": "2", "format": None},
                            {"name": "cell", "format": None},
                            {"name": "jobs", "format": None},
                        ]
                    },
                    "compareMode": False,
                    "compareType": "absolute",
                    "enabled": True,
                },
                "brush": {"size": 0.5, "enabled": False},
                "geocoder": {"enabled": False},
                "coordinate": {"enabled": False},
            },
            "layerBlending": "normal",
            "overlayBlending": "normal",
            "splitMaps": [],
            "animationConfig": {"currentTime": None, "speed": 1},
            "editor": {"features": [], "visible": True},
        },
        "mapState": {
            "bearing": 0,
            "dragRotate": False,
            "latitude": 25.98112631733372,
            "longitude": -80.21566191956265,
            "pitch": 0,
            "zoom": 9.261701808045437,
            "isSplit": False,
            "isViewportSynced": True,
            "isZoomLocked": False,
            "splitMapViewports": [],
        },
        "mapStyle": {
            "styleType": "dark",
            "topLayerGroups": {},
            "visibleLayerGroups": {
                "label": True,
                "road": True,
                "border": False,
                "building": True,
                "water": True,
                "land": True,
                "3d building": False,
            },
            "threeDBuildingColor": [15.035172933000911, 15.035172933000911, 15.035172933000911],
            "backgroundColor": [0, 0, 0],
            "mapStyles": {},
        },
    },
})
keplergl_static(map_1)
