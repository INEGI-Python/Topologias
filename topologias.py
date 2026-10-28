import argparse
import tkinter as tk
from tkinter import ttk
import geopandas as geo
import dask_geopandas as dgeo
from dask.distributed import Client
from shapely.geometry import Point
#import matplotlib.pyplot as plt
from time import time as t
from reglas import topologicas


class Topo:
    def __init__(self,ruta,capa,datos):
        self.ruta = ruta
        self.capa = capa
        self.reglas = datos
        self.data_sort = self.cargarDatos()
        self.errores = {}

    def cargarDatos(self):
        print(f"cargando capa {self.capa}")
        capa = dgeo.read_parquet(f"{self.ruta}{self.capa}.parquet",columns=['id','geometry'])
        return capa.spatial_shuffle(by="hilbert").compute()
    def noSeparado(self,k):
        print(f"Si la capa {self.capa.upper()} se {self.reglas[k].get('funcion').split('(')[0]} con las capas  {self.reglas[k].get('capas',None)}, usando una tolerancia de {self.reglas[k].get('tolerancia')} con las siguientes excepciones {self.reglas[k].get('excepciones')}")
    def fueraDe(self,k):
        ini=t()
        print(f"Si la capa {self.capa.upper()} se encuentra {k} la capa  {self.reglas[k].get('capas',None)[0]}, usando una tolerancia de {self.reglas[k].get('tolerancia', 0.0 )} con las siguientes excepciones {self.reglas[k].get('excepciones',"NINGUNA")}")
        pol_nal = dgeo.read_parquet(f"{self.ruta}{self.reglas[k].get('capas')[0]}.parquet",columns=['geoid','geometry']).to_crs(self.data_sort.crs)
        pol_nal_ordenado = pol_nal.spatial_shuffle(by="hilbert")
        dentro = dgeo.sjoin(self.data_sort,pol_nal_ordenado,  how="inner", predicate="within")
        idx_dentro = list(dentro.index)
        fuera=[i for i in list(self.data_sort.index) if i not in idx_dentro]
        self.data_sort.loc[fuera].to_file(f"{self.ruta}errores_{self.capa}.gpkg",layer=k,driver="GPKG")
        self.errores[k] = dict(cantidad=len(fuera),tiempo=f"{round(t()-ini,2)} segundos")

    def dangles(self,k):
        ini=t()
        capaValidar = self.data_sort.copy()
        tol = self.reglas.get(k, {}).get('tolerancia', 0.0)

        # Recorremos cada geometría de la capa y tomamos los extremos de cada tramo
        puntos = []
        for geom in capaValidar.geometry:
            if geom is None or geom.is_empty:
                continue

            if geom.geom_type in ('LineString', 'LinearRing'):
                coords = list(geom.coords)
                puntos.append(Point(coords[0]))
                puntos.append(Point(coords[-1]))

            elif geom.geom_type == 'MultiLineString':
                for parte in geom.geoms:
                    if parte.is_empty:
                        continue
                    coords = list(parte.coords)
                    puntos.append(Point(coords[0]))
                    puntos.append(Point(coords[-1]))

        # Contamos cuántas veces aparece cada extremo. Si un punto aparece solo una vez,
        # es un pseudonodo (extremo libre) dentro de la capa.
        conteo = {}
        for p in puntos:
            key = (round(p.x, 8), round(p.y, 8))
            conteo[key] = conteo.get(key, 0) + 1

        dangles = [Point(x, y) for (x, y), n in conteo.items() if n == 1]
        resultado=geo.GeoDataFrame(geometry=dangles, crs=capaValidar.crs)
        resultado.to_file(f"{self.ruta}errores_{self.capa}.gpkg",layer=k,driver="GPKG")
        self.errores[k] = dict(cantidad=len(dangles),tiempo=f"{round(t()-ini,2)} segundos")
        return resultado

    def pseudonodos(self,k):
        ini=t()
        dangles=self.dangles("dangles")
        d_dangles = dgeo.from_geopandas(dangles)
        d_dangles_ord = d_dangles.spatial_shuffle(by="hilbert")
        if self.reglas[k].get("tolerancia", 0) > 0:
            buffer = self.data_sort.buffer(self.reglas[k].get("tolerancia", 0))
            buffer = geo.GeoDataFrame(geometry=buffer, crs=self.data_sort.crs)
        else:
            buffer = self.data_sort.copy()  # Si no hay tolerancia, usamos la capa original para la intersección
        sjoin = dgeo.sjoin(d_dangles_ord, buffer, predicate="intersects")
        print(sjoin.head(5))
        conteo={}
        for geom in sjoin.geometry:
            key = (round(geom.x, 8), round(geom.y, 8))
            conteo[key] = conteo.get(key, 0) + 1
        pseudo = [Point(x, y) for (x, y), n in conteo.items() if n == 2]
        resultado=geo.GeoDataFrame(geometry=pseudo, crs=self.data_sort.crs)
        print(resultado)  
        resultado.to_file(f"{self.ruta}errores_{self.capa}.gpkg",layer=k,driver="GPKG")
        self.errores[k] = dict(cantidad=len(pseudo),tiempo=f"{round(t()-ini,2)} segundos") 
        return resultado

    def sobreponerse(self,k):
        print(f"Si la capa {self.capa.upper()} tiene uno o mas o mas elementos de la misma capa sobrepuestos entre ellos -  {k} ")
        ini = t()
        datos = self.data_sort
        if datos.empty:
            resultado = datos.copy()
        else:
            # Use a positional identifier so this also works when the source
            # index is duplicated or has a non-integer type.
            izquierda = datos[['geometry']].copy()
            derecha = datos[['geometry']].copy()
            izquierda['_id_izq'] = range(len(izquierda))
            derecha['_id_der'] = range(len(derecha))
            pares = geo.sjoin(
                izquierda, derecha, how='inner', predicate='overlaps',
                lsuffix='izq', rsuffix='der'
            )
            pares = pares[pares['_id_izq'] < pares['_id_der']]
            posiciones = set(pares['_id_izq'].tolist()) | set(pares['_id_der'].tolist())
            resultado = datos.iloc[sorted(posiciones)].copy() if posiciones else datos.iloc[0:0].copy()

        if not resultado.empty:
            resultado.to_file(f"{self.ruta}errores_{self.capa}.gpkg", layer=k, driver="GPKG")
        cantidad = len(resultado)
        print(f"Se encontraron {cantidad} geometrías de {self.capa.upper()} sobrepuestas entre sí.")
        self.errores[k] = dict(cantidad=cantidad, tiempo=f"{round(t()-ini,2)} segundos")
        return resultado
        
    def sobreponerse_mismo(self,k):
        print(f"Si la capa {self.capa.upper()} tiene algun elemento que se sobrepone a si mismo - {k}")
        ini = t()
        resultado = self.data_sort.iloc[
            [i for i, geom in enumerate(self.data_sort.geometry)
             if geom is not None and not geom.is_empty and
             ((geom.geom_type in ('LineString', 'LinearRing', 'MultiLineString') and not geom.is_simple)
              or (geom.geom_type in ('Polygon', 'MultiPolygon') and not geom.is_valid))]
        ].copy()
        if not resultado.empty:
            resultado.to_file(f"{self.ruta}errores_{self.capa}.gpkg", layer=k, driver="GPKG")
        cantidad = len(resultado)
        print(f"Se encontraron {cantidad} geometrías con autointersecciones.")
        self.errores[k] = dict(cantidad=cantidad, tiempo=f"{round(t()-ini,2)} segundos")
        return resultado
        
    def intersecar_mismo(self,k):
        print(f"Si la capa {self.capa.upper()} tiene algun elemento que se intersecta a si mismo - {k}")
        ini = t()
        posiciones = [
            i for i, geom in enumerate(self.data_sort.geometry)
            if geom is not None and not geom.is_empty
            and geom.geom_type in ('LineString', 'LinearRing', 'MultiLineString')
            and not geom.is_simple
        ]
        resultado = self.data_sort.iloc[posiciones].copy()
        if not resultado.empty:
            resultado.to_file(f"{self.ruta}errores_{self.capa}.gpkg", layer=k, driver="GPKG")
        cantidad = len(resultado)
        print(f"Se encontraron {cantidad} geometrías lineales que se intersectan a sí mismas.")
        self.errores[k] = dict(cantidad=cantidad, tiempo=f"{round(t()-ini,2)} segundos")
        return resultado


    def multiparte(self,k):
        ini=t()
        multiparte = []
        for idx, geom in self.data_sort.geometry.items():
            if geom is None or geom.is_empty:
                continue
            if geom.geom_type.startswith("Multi"):
                multiparte.append(idx)
        resultado = self.data_sort.loc[multiparte].copy()
        if not resultado.empty:
            resultado.to_file(f"{self.ruta}errores_{self.capa}.gpkg", layer=k, driver="GPKG")
        print(f"Si la capa {self.capa.upper()} tiene algun elemento que sea {k}. Se encontraron {len(multiparte)} geometrías multiparte.")
        self.errores[k] = dict(cantidad=len(multiparte), tiempo=f"{round(t()-ini,2)} segundos")
    def sobreponerse_con(self,k):
        ini=t()
        capas = self.reglas[k].get('capas', [])
        print(f"Si la capa {self.capa.upper()} se sobrepone con alguna de las capas {capas}")

        if not capas:
            self.errores[k] = dict(cantidad=0, tiempo=f"{round(t()-ini,2)} segundos")
            return geo.GeoDataFrame(geometry=[], crs=self.data_sort.crs)

        indices = set()
        for capa_comp in capas:
            capa_comp = dgeo.read_parquet(f"{self.ruta}{capa_comp}.parquet", columns=['geometry'])
            capa_comp = capa_comp.to_crs(self.data_sort.crs)
            sjoin = dgeo.sjoin(self.data_sort, capa_comp, how="inner", predicate="overlaps")

            sjoin = sjoin.compute()
            if not sjoin.empty:
                indices.update(sjoin.index.tolist())

        resultado = self.data_sort.loc[list(indices)].copy() if indices else self.data_sort.iloc[0:0].copy()
        if not resultado.empty:
            resultado.to_file(f"{self.ruta}errores_{self.capa}.gpkg", layer=k, driver="GPKG")

        print(f"Se encontraron {len(resultado)} geometrías de {self.capa.upper()} superpuestas con otras capas.")
        self.errores[k] = dict(cantidad=len(resultado), tiempo=f"{round(t()-ini,2)} segundos")
        return resultado
 
    def extremos_no_conectan(self,k):
        print(f"Si la capa {self.capa.upper()} en alguno de sus extremos no interseca con algun elemento de las capas {self.reglas[k].get('capas')}")
        ini = t()
        capas = self.reglas[k].get('capas', [])
        tolerancia = self.reglas[k].get('tolerancia', 0.0)

        # Crear un punto por cada extremo de cada geometría lineal.
        extremos = []
        for idx, geom in self.data_sort.geometry.items():
            if geom is None or geom.is_empty:
                continue
            partes = [geom] if geom.geom_type in ('LineString', 'LinearRing') else (
                list(geom.geoms) if geom.geom_type == 'MultiLineString' else [])
            for parte in partes:
                if parte.is_empty:
                    continue
                coords = list(parte.coords)
                for coord in (coords[0], coords[-1]):
                    extremos.append({'origen': idx, 'geometry': Point(coord)})

        puntos = geo.GeoDataFrame(extremos, geometry='geometry', crs=self.data_sort.crs)
        if tolerancia > 0 and not puntos.empty:
            puntos['geometry'] = puntos.geometry.buffer(tolerancia)

        conectados = set()
        if not puntos.empty:
            puntos['extremo_id'] = range(len(puntos))
            for nombre_capa in capas:
                comparacion = dgeo.read_parquet(
                    f"{self.ruta}{nombre_capa}.parquet", columns=['geometry']
                ).to_crs(self.data_sort.crs)
                unidos = dgeo.sjoin(
                    dgeo.from_geopandas(puntos), comparacion,
                    how='inner', predicate='intersects'
                ).compute()
                conectados.update(unidos['extremo_id'].tolist())

        errores = puntos.loc[~puntos['extremo_id'].isin(conectados)].copy() if not puntos.empty else puntos
        if not errores.empty:
            errores.to_file(f"{self.ruta}errores_{self.capa}.gpkg", layer=k, driver="GPKG")
        self.errores[k] = dict(cantidad=len(errores), tiempo=f"{round(t()-ini,2)} segundos")
        return errores






def inicio(_a):
    topo = Topo(_a.folder_datos, _a.capa, topologicas.get(_a.capa, []))
    for k in topo.reglas.keys():
        print("* * "*20)
        exec(topo.reglas[k]["funcion"])
    key = topo.errores.keys()
    if len(key)>0:
        print(" ERRORES TOPOLOGICOS ENCONTRADOS ")
        print(topo.errores)


def ventana_topologias():
    ventana = tk.Tk()
    ventana.title("Topologías disponibles")
    ventana.geometry("350x450")
    tk.Label(ventana, text="Capas a validar").pack(pady=8)
    selector = ttk.Combobox(ventana, values=list(topologicas), state="readonly")
    selector.pack(fill=tk.X, padx=10, pady=10)

    def ejecutar(_evento=None):
        capa = selector.get()
        if capa:
            ventana.destroy()
            inicio(argparse.Namespace(capa=capa))

    selector.bind("<<ComboboxSelected>>", ejecutar)
    ventana.mainloop()
        

if __name__=="__main__":
    args = argparse.ArgumentParser(description="Validacion de reglas topologicas")
    args.add_argument("--capa",type=str,help="Nombre de la capa a validar")
    _a = args.parse_args()
    if _a.capa:
        inicio(_a)
    else:
        ventana_topologias()





#  python reglas.py --capa 


    # ['acueducto_l', 'aerodromo_a', 'area_publica_a', 'banco_mater_p', 'banco_nivel_p', 'bordo_l', 'camino_l', 'canal_a', 'canal_l', 'carretera_l', 'cementerio_a', 'cementerio_p', 'conducto_l', 'corriente_ag_a',
    # 'corriente_ag_l', 'corriente_de_p', 'cuerpo_agua_a', 'curva_nivel_l', 'deposito_des_p', 'edifica_cult_a', 'edifica_cult_p', 'edificacion_a', 'edificacion_p', 'elemento_ins_a', 'elemento_ins_p', 'emisor_senal_p',
    # 'enlace_fluvi_l', 'entrada_cavi_p', 'escuela_a', 'escuela_p', 'est_ferrocarril_a', 'est_ferrocarril_p', 'est_geo_grav_p', 'est_geo_hor_p', 'estacion_abast_p', 'estacion_tel_p', 'estanque_a', 'estruc_eleva_p', 
    # 'fango_a', 'form_rel_su_a', 'form_rel_su_p', 'funicular_l', 'granja_a', 'ins_aduanal_a', 'ins_aduanal_p', 'ins_bombeo_a', 'ins_bombeo_p', 'ins_deportiv_a', 'ins_deportiv_p', 'ins_diversa_a', 'ins_diversa_p', 
    # 'ins_industr_a', 'ins_portuar_a', 'ins_portuar_l', 'limite_intna_l', 'linea_costa_l', 'linea_transm_l', 'localidad_a', 'localidad_p', 'malpais_a', 'manantial_p', 'manzana_a', 'mina_p', 'mojonera_p', 'muro_contenc_l',
    # 'pantano_a', 'pista_aviaci_a', 'pista_aviaci_l', 'pista_carrer_l', 'planta_agua_a', 'planta_gener_a', 'planta_gener_p', 'plaza_cobro_p', 'pozo_p', 'presa_l', 'puente_l', 'punto_acotad_p', 'rasgo_arqueo_a', 
    # 'rasgo_arqueo_p', 'referencia_g_p', 'salina_a', 'salto_agua_l', 'salto_agua_p', 'servicio_med_a', 'servicio_med_p', 'subesta_elec_a', 'subesta_elec_p', 'tanque_a', 'tanque_p', 'terreno_suje_a', 'tunel_l', 'vado_p', 
    # 'via_ferrea_l', 'vialidad_l', 'zona_a']