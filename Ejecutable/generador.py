import pandas as pd
import os
import xml.etree.ElementTree as ET

# LOCAL IMPORTS

from functions import kml_df, gen_ofertas, gen_solicitudes

if __name__ == '__main__':

    df = kml_df('coordenadas')

    dfs_list = gen_ofertas(df)

    solicitudes = gen_solicitudes(dfs_list)

    print(dfs_list)
    print(solicitudes)