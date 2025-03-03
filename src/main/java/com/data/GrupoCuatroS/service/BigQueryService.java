package com.data.GrupoCuatroS.service;

import com.data.GrupoCuatroS.entity.BigQueryResulteEntity;
import com.data.GrupoCuatroS.entity.CatalogResultEntity;
import com.data.GrupoCuatroS.entity.ResumenViviendaResultEntity;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

@Service
public class BigQueryService {

    private static final String CREDENTIALS_PATH = "credentials/bigquery-credentials.json";

    public List<BigQueryResulteEntity> consultarConFiltros( String estado,
												            String tipo,
												            String zona,
												            String segmento,
												            String precioFichaIni,
												            String precioFichaFin,
												            String preciom2Ini,
												            String preciom2Fin,
												            String periodoScrap,
												            int salientes,
												            String latBusc,
												            String lngBusc,
												            String kmBusc) {
        // Consulta SQL construida dinámicamente
        String query =
                "SELECT ROW_NUMBER() OVER () AS idConsulta, "
            			+ " url, "
                        + " dias_pub, "
                        + " antiguedad, "
                        + " banos, "
                        + " descripcion, "
                        + " dormitorios, "
                        + " fecha_scrap, "
                        + " ficha_precio, "
                        + " fuente, "
                        + " garages, "
                        + " lat, "
                        + " lng, "
                        + " loc_ciudad, "
                        + " loc_direccion, "
                        + " loc_estado, "
                        + " loc_tipo, "
                        + " loc_zona, "
                        + " moneda_pub, "
                        + " nombre, "
                        + " precio_pub, "
                        + " segmento, "
                        + " superficie_cubierta, "
                        + " superficie_total, "
                        + " webscrap_id, "
                        + " CVEGEO, "
                        + " zona, "
                        + " CASE  "
                        + "     WHEN superficie_cubierta IS NULL THEN NULL  "
                        + "     WHEN superficie_cubierta = 0 THEN NULL  "
                        + "     ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) "
                        + " END AS precio_m2 "
                        + " FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` ";
        if(salientes == 0)                
        	query += " WHERE estatus IN ('entrante', 'existente') ";
        if(salientes == 1)
        	query += " WHERE estatus IN ('saliente') ";
        
    	if(!periodoScrap.isEmpty()) {
    		periodoScrap = periodoScrap.replaceAll(",","','");
    		query += " AND periodo IN ('" + periodoScrap + "')";
    	}
    	
    	if(!estado.isEmpty()) {
    		estado = estado.replaceAll(",","','");
    		query += " AND loc_estado IN ('" + estado + "')";
    	}
    	
    	if(!tipo.isEmpty()) {
    		tipo = tipo.replaceAll(",","','");
    		query += " AND loc_tipo IN (";
    		String queryTipo = "";
    		
    		if(tipo.contains("VV"))
    			queryTipo += " 'Departamento','Departamento','Departamentos','Departamentos en Venta'";
    		
    		if(tipo.contains("VH")) {
    			if(queryTipo.length() != 0)
    				queryTipo += ",";
    			
    			queryTipo += " 'Casas'";
    		}
    		
    		if(tipo.contains("L")) {
    			if(queryTipo.length() != 0)
    				queryTipo += ",";
    			
    			queryTipo += " 'Locales Comerciales'";
    		}
    		
    		if(tipo.contains("O")) {
    			if(queryTipo.length() != 0)
    				queryTipo += ",";
    			
    			queryTipo += " 'Oficinas'";
    		}
    		
    		if(tipo.contains("B")) {
    			if(queryTipo.length() != 0)
    				queryTipo += ",";
    			
    			queryTipo += " 'Bodegas'";
    		}
    		
    		if(tipo.contains("S")) {
    			if(queryTipo.length() != 0)
    				queryTipo += ",";
    			
    			queryTipo += " '...', 'N/A','Venta'";
    		}
        		
        	query += queryTipo + ")";
    		
    	}
    	
    	if(!zona.isEmpty()) {
    		zona = zona.replaceAll(",","','");
    		query += " AND zona IN ('" + zona + "')";
    	}
    	
    	if(!segmento.isEmpty()) {
    		segmento = segmento.replaceAll(",","','");
    		query += " AND segmento IN ('" + segmento + "')";
    	}
    	
    	if(!precioFichaIni.isEmpty()) {
    		query += " AND ficha_precio >= " + precioFichaIni;
    	}
    	
    	if(!precioFichaFin.isEmpty()) {
    		query += " AND ficha_precio <= " + precioFichaFin;
    	}
    	
    	if(!preciom2Ini.isEmpty()) {
    		query += " AND CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END >= " + preciom2Ini;
    	}
    	
    	if(!preciom2Fin.isEmpty()) {
    		query += " AND CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END <= " + preciom2Fin;
    	}
        	
        if(!latBusc.isEmpty() && !lngBusc.isEmpty()) {
        	query += " AND lat <= (" + latBusc + " + " + kmBusc + ") AND lat >= (" + latBusc + " - " + kmBusc + ") "
        			+ " AND lng <= (" + lngBusc + " + " + kmBusc + ") AND lng >= (" + lngBusc + " - " + kmBusc + ") ";
        }
        	
    	
        return executeQueryBigData(query);
    }
    
    public List<CatalogResultEntity> getEstados() {
        // Consulta SQL construida dinámicamente
        String query =
                "SELECT DISTINCT loc_estado AS catalogName"
                + "   FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` "
                + "  ORDER BY 1";

        return executeQueryCatalogos(query);
    }
    
    public List<CatalogResultEntity> getZona(String estadoWhere) {
    	String queryWhere = " ";
    	String estados = "";
    	
    	if(!estadoWhere.isEmpty()) {
    		estados = estadoWhere.replaceAll(",","','");
    		queryWhere = " WHERE loc_estado IN ('" + estados + "') ";
    		
    	}
        // Consulta SQL construida dinámicamente
        String query =
                "SELECT DISTINCT zona AS catalogName"
                + "   FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` "
                + queryWhere
                + "  ORDER BY 1";

        return executeQueryCatalogos(query);
    }
    
    public List<CatalogResultEntity> getSegmento() {
        // Consulta SQL construida dinámicamente
        String query =
                "SELECT DISTINCT segmento AS catalogName"
                + "   FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` "
                + "  ORDER BY 1";

        return executeQueryCatalogos(query);
    }
    
    public List<CatalogResultEntity> getPeriodo() {
        // Consulta SQL construida dinámicamente
        String query =
                "SELECT DISTINCT periodo AS catalogName "
                + "  FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` "
                + " ORDER BY 1 desc";

        return executeQueryCatalogos(query);
    }
    
    public JSONObject getDormitoriosRangos(String estado,
    										 String tipo,
								             String zona,
								             String segmento,
								             String iniBusc,
								             String finBusc,
								             String periodoScrap,
								             String rangosPrecio,
								             String indicador,
								             String latBusc,
								             String lngBusc,
								             String kmBusc) {
        
    	List<Integer> rangos; 
    	rangos = dividirXMontos(Integer.valueOf(iniBusc), Integer.valueOf(finBusc), Integer.valueOf(rangosPrecio));
    	
    	ArrayList<String> headersTabla = new ArrayList<String>();
    	headersTabla.add("loc_estado");
    	headersTabla.add("zona");
    	
    	String queryWhere = " ";
    	
    	if(!estado.isEmpty() || !tipo.isEmpty() || !zona.isEmpty() || !segmento.isEmpty() || !iniBusc.isEmpty() || !finBusc.isEmpty() || !periodoScrap.isEmpty()) {
    		queryWhere += "  WHERE estatus IN ('entrante', 'existente') ";
        	
        	if(!periodoScrap.isEmpty()) {
        		periodoScrap = periodoScrap.replaceAll(",","','");
        		queryWhere += " AND periodo IN ('" + periodoScrap + "') ";
        	}
        	
        	if(!estado.isEmpty()) {
        		estado = estado.replaceAll(",","','");
        		queryWhere += " AND loc_estado IN ('" + estado + "') ";
        	}
        	
        	if(!tipo.isEmpty()) {
        		tipo = tipo.replaceAll(",","','");
        		queryWhere += " AND loc_tipo IN (";
        		String queryTipo = "";
        		
        		if(tipo.contains("VV"))
        			queryTipo += " 'Departamento','Departamento','Departamentos','Departamentos en Venta'";
        		
        		if(tipo.contains("VH")) {
        			if(queryTipo.length() != 0)
        				queryTipo += ",";
        			
        			queryTipo += " 'Casas'";
        		}
        		
        		if(tipo.contains("L")) {
        			if(queryTipo.length() != 0)
        				queryTipo += ",";
        			
        			queryTipo += " 'Locales Comerciales'";
        		}
        		
        		if(tipo.contains("O")) {
        			if(queryTipo.length() != 0)
        				queryTipo += ",";
        			
        			queryTipo += " 'Oficinas'";
        		}
        		
        		if(tipo.contains("B")) {
        			if(queryTipo.length() != 0)
        				queryTipo += ",";
        			
        			queryTipo += " 'Bodegas'";
        		}
        		
        		if(tipo.contains("S")) {
        			if(queryTipo.length() != 0)
        				queryTipo += ",";
        			
        			queryTipo += " '...', 'N/A','Venta'";
        		}
            		
        		queryWhere += queryTipo + ")";
        		
        		
        	}
        	
        	if(!zona.isEmpty()) {
        		zona = zona.replaceAll(",","','");
        		queryWhere += " AND zona IN ('" + zona + "')";
        	}
        	
        	if(!segmento.isEmpty()) {
        		segmento = segmento.replaceAll(",","','");
        		queryWhere += " AND segmento IN ('" + segmento + "') ";
        	}
        	
        	if(Integer.valueOf(indicador) == 1) {
	        	if(!iniBusc.isEmpty()) {
	        		queryWhere += " AND ficha_precio >= " + iniBusc;
	        	}
	        	
	        	if(!finBusc.isEmpty()) {
	        		queryWhere += " AND ficha_precio <= " + finBusc;
	        	}
        	}
        	
        	if(Integer.valueOf(indicador) == 2) {
	        	if(!iniBusc.isEmpty()) {
	        		 queryWhere += " AND CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END >= " + iniBusc;
	        	}
	        	
	        	if(!finBusc.isEmpty()) {
	        		queryWhere += " AND CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END <= " + finBusc;
	        	}
        	}
        	
        	if(Integer.valueOf(indicador) == 3) {
	        	if(!iniBusc.isEmpty()) {
	        		queryWhere += " AND superficie_cubierta >= " + iniBusc;
	        	}
	        	
	        	if(!finBusc.isEmpty()) {
	        		queryWhere += " AND superficie_cubierta <= " + finBusc;
	        	}
        	}
        	
        	//AGREGANDO ESTA SECCION AYUDA A NO REGRESAR NADA SI LOS CAMPOS NO FUERON ENVIADOS
        	if(Integer.valueOf(indicador) == 0) {
	        	queryWhere += " AND url IN ('NO REGRESAR NADA') ";
        	}
        	
        	if(!latBusc.isEmpty() && !lngBusc.isEmpty()) {
        		queryWhere += " AND lat <= (" + latBusc + " + " + kmBusc + ") AND lat >= (" + latBusc + " - " + kmBusc + ") "
            				+ " AND lng <= (" + lngBusc + " + " + kmBusc + ") AND lng >= (" + lngBusc + " - " + kmBusc + ") ";
            }
        
        }
    	
    	
        String query = ""; 
        		
        if(tipo.contains("VH")) {
	        query	= "WITH opciones_dormitorios AS ("
	        		+ "    SELECT DISTINCT"
	        		+ "        loc_estado,"
	        		+ "        zona,"
	        		+ "        opcion_dormitorios"
	        		+ "    FROM ("
	        		+ "        SELECT "
	        		+ "            loc_estado,"
	        		+ "            zona,"
	        		+ "            '2 dormitorios o menos' AS opcion_dormitorios"
	        		+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1`"
	        		+ queryWhere
	        		+ "        UNION ALL"
	        		+ "        SELECT "
	        		+ "            loc_estado,"
	        		+ "            zona,"
	        		+ "            '3 dormitorios' AS opcion_dormitorios"
	        		+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1`"
	        		+ queryWhere
	        		+ "        UNION ALL"
	        		+ "        SELECT "
	        		+ "            loc_estado,"
	        		+ "            zona,"
	        		+ "            '4 dormitorios' AS opcion_dormitorios"
	        		+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1`"
	        		+ queryWhere
	        		+ "        UNION ALL"
	        		+ "        SELECT "
	        		+ "            loc_estado,"
	        		+ "            zona,"
	        		+ "            'MAS de 4 dormitorios' AS opcion_dormitorios"
	        		+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` "
	        		+ queryWhere
	        		+ "    )"
	        		+ "),"
	        		+ "datos_agrupados AS ("
	        		+ "    SELECT "
	        		+ "        loc_estado,"
	        		+ "        zona,"
	        		+ "        CASE"
	        		+ "            WHEN dormitorios <= 2 THEN '2 dormitorios o menos'"
	        		+ "            WHEN dormitorios = 3 THEN '3 dormitorios'"
	        		+ "            WHEN dormitorios = 4 THEN '4 dormitorios'"
	        		+ "            WHEN dormitorios > 4 THEN 'MAS de 4 dormitorios'"
	        		+ "        END AS dormitorios_cantidad, "
	        		+ "        CASE  ";
        }
        
        if(tipo.contains("VV")) {
	        query	= "WITH opciones_dormitorios AS ("
	        		+ "    SELECT DISTINCT"
	        		+ "        loc_estado,"
	        		+ "        zona,"
	        		+ "        opcion_dormitorios"
	        		+ "    FROM ("
	        		+ "        SELECT "
	        		+ "            loc_estado,"
	        		+ "            zona,"
	        		+ "            '1 dormitorio' AS opcion_dormitorios"
	        		+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1`"
	        		+ queryWhere
	        		+ "        UNION ALL"
	        		+ "        SELECT "
	        		+ "            loc_estado,"
	        		+ "            zona,"
	        		+ "            '2 dormitorios' AS opcion_dormitorios"
	        		+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1`"
	        		+ queryWhere
	        		+ "        UNION ALL"
	        		+ "        SELECT "
	        		+ "            loc_estado,"
	        		+ "            zona,"
	        		+ "            '3 dormitorios' AS opcion_dormitorios"
	        		+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1`"
	        		+ queryWhere
	        		+ "        UNION ALL"
	        		+ "        SELECT "
	        		+ "            loc_estado,"
	        		+ "            zona,"
	        		+ "            'MAS de 3 dormitorios' AS opcion_dormitorios"
	        		+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` "
	        		+ queryWhere
	        		+ "    )"
	        		+ "),"
	        		+ "datos_agrupados AS ("
	        		+ "    SELECT "
	        		+ "        loc_estado,"
	        		+ "        zona,"
	        		+ "        CASE"
	        		+ "            WHEN dormitorios = 1 THEN '1 dormitorio'"
	        		+ "            WHEN dormitorios = 2 THEN '2 dormitorios'"
	        		+ "            WHEN dormitorios = 3 THEN '3 dormitorios'"
	        		+ "            WHEN dormitorios > 3 THEN 'MAS de 3 dormitorios'"
	        		+ "        END AS dormitorios_cantidad, "
	        		+ "        CASE  ";
        }
        
        for (int i = 0; i < rangos.size(); i++) {	
        	if(i == 0) {
        		headersTabla.add("r" + (rangos.get(i) - 1));
        		if(Integer.valueOf(indicador) == 1) {
        			query += " WHEN ficha_precio < " + rangos.get(i) + " THEN 'r" + (rangos.get(i) - 1) + "' ";
        		}
        		if(Integer.valueOf(indicador) == 2) {
        			query += " WHEN CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END < " + rangos.get(i) + " THEN 'r" + (rangos.get(i) - 1) + "' ";
        		}
        		if(Integer.valueOf(indicador) == 3) {
        			query += " WHEN superficie_cubierta < " + rangos.get(i) + " THEN 'r" + (rangos.get(i) - 1) + "' ";
        		}
        		//SE AGREGA PARA NO MOSTRAR DATOS
        		if(Integer.valueOf(indicador) == 0) {
        			query += " WHEN ficha_precio = 1 THEN 'r' ";
        		}
        	}else if (i+1 == rangos.size()) {
        		headersTabla.add("r" + (rangos.get(i)));
        		if(Integer.valueOf(indicador) == 1) {
        			query += " WHEN ficha_precio BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
            		query += " ELSE 'r" + rangos.get(i) + "'";
        		}
        		if(Integer.valueOf(indicador) == 2) {
        			query += " WHEN CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
            		query += " ELSE 'r" + rangos.get(i) + "'";
        		}
        		if(Integer.valueOf(indicador) == 3) {
        			query += " WHEN superficie_cubierta BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
            		query += " ELSE 'r" + rangos.get(i) + "'";
        		}
        		//SE AGREGA PARA NO MOSTRAR DATOS
        		if(Integer.valueOf(indicador) == 0) {
        			query += " WHEN ficha_precio = 1 THEN 'r' ";
        		}
        	}else {
        		headersTabla.add("r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1));
        		if(Integer.valueOf(indicador) == 1) {
        			query += " WHEN ficha_precio BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
        		}
        		if(Integer.valueOf(indicador) == 2) {
        			query += " WHEN CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
        		}
        		if(Integer.valueOf(indicador) == 3) {
        			query += " WHEN superficie_cubierta BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
        		}
        		//SE AGREGA PARA NO MOSTRAR DATOS
        		if(Integer.valueOf(indicador) == 0) {
        			query += " WHEN ficha_precio = 1 THEN 'r' ";
        		}
        	}
        }
        
        query  += "      END AS rango_precio"
        		+ "    FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` "
        		+ queryWhere;
        
        
        
        query  += "),"
        		+ "conteo_datos AS ("
        		+ "    SELECT "
        		+ "        loc_estado,"
        		+ "        zona,"
        		+ "        dormitorios_cantidad, ";
        
        for (int i = 0; i < rangos.size(); i++) {	
        	if(i == 0) {
        		query += " COUNTIF(rango_precio = 'r" + (rangos.get(i) - 1) + "') AS `r" + (rangos.get(i) - 1) + "`, ";
        	}else if (i+1 == rangos.size()) {
        		query += " COUNTIF(rango_precio = 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "') AS `r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "`, ";
        		query += " COUNTIF(rango_precio = 'r" + rangos.get(i) + "') AS `r" + rangos.get(i) + "` ";
        	}else {
        		query += " COUNTIF(rango_precio = 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "') AS `r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "`, ";
        	}
        }
        query  += " FROM datos_agrupados "
        		+ "    GROUP BY loc_estado, zona, dormitorios_cantidad "
        		+ ") "
        		+ "SELECT "
        		+ "    o.loc_estado, "
        		+ "    o.zona, "
        		+ "    o.opcion_dormitorios AS dormitorios_cantidad, ";
        for (int i = 0; i < rangos.size(); i++) {	
        	if(i == 0) {
        		query += " COALESCE(c.r" + (rangos.get(i) - 1) + ", 0) AS `r" + (rangos.get(i) - 1) + "`, ";
        	}else if (i+1 == rangos.size()) {
        		query += " COALESCE(c.r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + ", 0) AS `r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "`, ";
        		query += " COALESCE(c.r" + rangos.get(i) + ", 0) AS `r" + rangos.get(i) + "` ";
        	}else {
        		query += " COALESCE(c.r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + ", 0) AS `r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "`, ";
        	}
        }
        query  += "FROM opciones_dormitorios o "
        		+ "LEFT JOIN conteo_datos c "
        		+ "ON o.loc_estado = c.loc_estado "
        		+ "   AND o.zona = c.zona "
        		+ "   AND o.opcion_dormitorios = c.dormitorios_cantidad "
        		+ "ORDER BY o.loc_estado, o.zona, o.opcion_dormitorios;";
        
        

        return executeQueryRangos(query, headersTabla);
    }
    
    public JSONObject getBaniosRangos(	String estado,
										String tipo,
									    String zona,
									    String segmento,
									    String iniBusc,
									    String finBusc,
									    String periodoScrap,
									    String rangosPrecio,
									    String indicador,
							            String latBusc,
							            String lngBusc,
							            String kmBusc) {

		List<Integer> rangos; 
		rangos = dividirXMontos(Integer.valueOf(iniBusc), Integer.valueOf(finBusc), Integer.valueOf(rangosPrecio));
		
		ArrayList<String> headersTabla = new ArrayList<String>();
		headersTabla.add("loc_estado");
		headersTabla.add("zona");
		
		String queryWhere = " ";
		
		if(!estado.isEmpty() || !tipo.isEmpty() || !zona.isEmpty() || !segmento.isEmpty() || !iniBusc.isEmpty() || !finBusc.isEmpty() || !periodoScrap.isEmpty()) {
			queryWhere += "  WHERE estatus IN ('entrante', 'existente') ";
			
			if(!periodoScrap.isEmpty()) {
				periodoScrap = periodoScrap.replaceAll(",","','");
				queryWhere += " AND periodo IN ('" + periodoScrap + "') ";
			}
			
			if(!estado.isEmpty()) {
				estado = estado.replaceAll(",","','");
				queryWhere += " AND loc_estado IN ('" + estado + "') ";
			}
			
			if(!tipo.isEmpty()) {
				tipo = tipo.replaceAll(",","','");
				queryWhere += " AND loc_tipo IN (";
				String queryTipo = "";
				
				if(tipo.contains("VV"))
					queryTipo += " 'Departamento','Departamento','Departamentos','Departamentos en Venta'";
				
				if(tipo.contains("VH")) {
					if(queryTipo.length() != 0)
						queryTipo += ",";
					
					queryTipo += " 'Casas'";
				}
			
				if(tipo.contains("L")) {
					if(queryTipo.length() != 0)
						queryTipo += ",";
					
					queryTipo += " 'Locales Comerciales'";
				}
				
				if(tipo.contains("O")) {
					if(queryTipo.length() != 0)
						queryTipo += ",";
					
					queryTipo += " 'Oficinas'";
				}
			
				if(tipo.contains("B")) {
					if(queryTipo.length() != 0)
						queryTipo += ",";
					
					queryTipo += " 'Bodegas'";
				}
			
				if(tipo.contains("S")) {
					if(queryTipo.length() != 0)
						queryTipo += ",";
					
					queryTipo += " '...', 'N/A','Venta'";
				}
				
				queryWhere += queryTipo + ")";
				
			
			}
			
			if(!zona.isEmpty()) {
				zona = zona.replaceAll(",","','");
				queryWhere += " AND zona IN ('" + zona + "')";
			}
			
			if(!segmento.isEmpty()) {
				segmento = segmento.replaceAll(",","','");
				queryWhere += " AND segmento IN ('" + segmento + "') ";
			}
			
			if(Integer.valueOf(indicador) == 1) {
				if(!iniBusc.isEmpty()) {
					queryWhere += " AND ficha_precio >= " + iniBusc;
				}
				
				if(!finBusc.isEmpty()) {
					queryWhere += " AND ficha_precio <= " + finBusc;
				}
			}
			
			if(Integer.valueOf(indicador) == 2) {
				if(!iniBusc.isEmpty()) {
					queryWhere += " AND CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END >= " + iniBusc;
				}
				
				if(!finBusc.isEmpty()) {
					queryWhere += " AND CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END <= " + finBusc;
				}
			}
			
			if(Integer.valueOf(indicador) == 3) {
				if(!iniBusc.isEmpty()) {
					queryWhere += " AND superficie_cubierta >= " + iniBusc;
				}
				
				if(!finBusc.isEmpty()) {
					queryWhere += " AND superficie_cubierta <= " + finBusc;
				}
			}
			
			//AGREGANDO ESTA SECCION AYUDA A NO REGRESAR NADA SI LOS CAMPOS NO FUERON ENVIADOS
			if(Integer.valueOf(indicador) == 0) {
				queryWhere += " AND url IN ('NO REGRESAR NADA') ";
			}
			
			if(!latBusc.isEmpty() && !lngBusc.isEmpty()) {
        		queryWhere += " AND lat <= (" + latBusc + " + " + kmBusc + ") AND lat >= (" + latBusc + " - " + kmBusc + ") "
            				+ " AND lng <= (" + lngBusc + " + " + kmBusc + ") AND lng >= (" + lngBusc + " - " + kmBusc + ") ";
            }
			
		}
		
		
		String query = ""; 
		
		if(tipo.contains("VH")) {
			query	= "WITH opciones_banos AS ("
					+ "    SELECT DISTINCT"
					+ "        loc_estado,"
					+ "        zona,"
					+ "        opcion_banos"
					+ "    FROM ("
					+ "        SELECT "
					+ "            loc_estado,"
					+ "            zona,"
					+ "            '2 banos o menos' AS opcion_banos"
					+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1`"
					+ queryWhere
					+ "        UNION ALL"
					+ "        SELECT "
					+ "            loc_estado,"
					+ "            zona,"
					+ "            '3 banos' AS opcion_banos"
					+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1`"
					+ queryWhere
					+ "        UNION ALL"
					+ "        SELECT "
					+ "            loc_estado,"
					+ "            zona,"
					+ "            '4 banos' AS opcion_banos"
					+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1`"
					+ queryWhere
					+ "        UNION ALL"
					+ "        SELECT "
					+ "            loc_estado,"
					+ "            zona,"
					+ "            'MAS de 4 banos' AS opcion_banos"
					+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` "
					+ queryWhere
					+ "    )"
					+ "),"
					+ "datos_agrupados AS ("
					+ "    SELECT "
					+ "        loc_estado,"
					+ "        zona,"
					+ "        CASE"
					+ "            WHEN banos <= 2 THEN '2 banos o menos'"
					+ "            WHEN banos = 3 THEN '3 banos'"
					+ "            WHEN banos = 4 THEN '4 banos'"
					+ "            WHEN banos > 4 THEN 'MAS de 4 banos'"
					+ "        END AS banos_cantidad, "
					+ "        CASE  ";
		}
		
		if(tipo.contains("VV")) {
			query	= "WITH opciones_banos AS ("
					+ "    SELECT DISTINCT"
					+ "        loc_estado,"
					+ "        zona,"
					+ "        opcion_banos"
					+ "    FROM ("
					+ "        SELECT "
					+ "            loc_estado,"
					+ "            zona,"
					+ "            '1 banos' AS opcion_banos"
					+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1`"
					+ queryWhere
					+ "        UNION ALL"
					+ "        SELECT "
					+ "            loc_estado,"
					+ "            zona,"
					+ "            '2 banos' AS opcion_banos"
					+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1`"
					+ queryWhere
					+ "        UNION ALL"
					+ "        SELECT "
					+ "            loc_estado,"
					+ "            zona,"
					+ "            '3 banos' AS opcion_banos"
					+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1`"
					+ queryWhere
					+ "        UNION ALL"
					+ "        SELECT "
					+ "            loc_estado,"
					+ "            zona,"
					+ "            'MAS de 3 banos' AS opcion_banos"
					+ "        FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` "
					+ queryWhere
					+ "    )"
					+ "),"
					+ "datos_agrupados AS ("
					+ "    SELECT "
					+ "        loc_estado,"
					+ "        zona,"
					+ "        CASE"
					+ "            WHEN banos = 1 THEN '1 banos'"
					+ "            WHEN banos = 2 THEN '2 banos'"
					+ "            WHEN banos = 3 THEN '3 banos'"
					+ "            WHEN banos > 3 THEN 'MAS de 3 banos'"
					+ "        END AS banos_cantidad, "
					+ "        CASE  ";
		}
		
		for (int i = 0; i < rangos.size(); i++) {	
			if(i == 0) {
				headersTabla.add("r" + (rangos.get(i) - 1));
				if(Integer.valueOf(indicador) == 1) {
					query += " WHEN ficha_precio < " + rangos.get(i) + " THEN 'r" + (rangos.get(i) - 1) + "' ";
				}
				if(Integer.valueOf(indicador) == 2) {
					query += " WHEN CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END < " + rangos.get(i) + " THEN 'r" + (rangos.get(i) - 1) + "' ";
				}
				if(Integer.valueOf(indicador) == 3) {
					query += " WHEN superficie_cubierta < " + rangos.get(i) + " THEN 'r" + (rangos.get(i) - 1) + "' ";
				}
				//SE AGREGA PARA NO MOSTRAR DATOS
				if(Integer.valueOf(indicador) == 0) {
					query += " WHEN ficha_precio = 1 THEN 'r' ";
				}
			}else if (i+1 == rangos.size()) {
				headersTabla.add("r" + (rangos.get(i)));
				if(Integer.valueOf(indicador) == 1) {
					query += " WHEN ficha_precio BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
					query += " ELSE 'r" + rangos.get(i) + "'";
				}
				if(Integer.valueOf(indicador) == 2) {
					query += " WHEN CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
					query += " ELSE 'r" + rangos.get(i) + "'";
				}
				if(Integer.valueOf(indicador) == 3) {
					query += " WHEN superficie_cubierta BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
					query += " ELSE 'r" + rangos.get(i) + "'";
				}
				//SE AGREGA PARA NO MOSTRAR DATOS
				if(Integer.valueOf(indicador) == 0) {
					query += " WHEN ficha_precio = 1 THEN 'r' ";
				}
			}else {
				headersTabla.add("r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1));
				if(Integer.valueOf(indicador) == 1) {
					query += " WHEN ficha_precio BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
				}
				if(Integer.valueOf(indicador) == 2) {
					query += " WHEN CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
				}
				if(Integer.valueOf(indicador) == 3) {
					query += " WHEN superficie_cubierta BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
				}
				//SE AGREGA PARA NO MOSTRAR DATOS
				if(Integer.valueOf(indicador) == 0) {
					query += " WHEN ficha_precio = 1 THEN 'r' ";
				}
			}
		}
		
		query  += "      END AS rango_precio"
		+ "    FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` "
		+ queryWhere;
		
		
		
		query  += "),"
		+ "conteo_datos AS ("
		+ "    SELECT "
		+ "        loc_estado,"
		+ "        zona,"
		+ "        banos_cantidad, ";
		
		for (int i = 0; i < rangos.size(); i++) {	
			if(i == 0) {
				query += " COUNTIF(rango_precio = 'r" + (rangos.get(i) - 1) + "') AS `r" + (rangos.get(i) - 1) + "`, ";
			}else if (i+1 == rangos.size()) {
				query += " COUNTIF(rango_precio = 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "') AS `r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "`, ";
				query += " COUNTIF(rango_precio = 'r" + rangos.get(i) + "') AS `r" + rangos.get(i) + "` ";
			}else {
				query += " COUNTIF(rango_precio = 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "') AS `r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "`, ";
			}
		}
		query  += " FROM datos_agrupados "
				+ "    GROUP BY loc_estado, zona, banos_cantidad "
				+ ") "
				+ "SELECT "
				+ "    o.loc_estado, "
				+ "    o.zona, "
				+ "    o.opcion_banos AS banos_cantidad, ";
		for (int i = 0; i < rangos.size(); i++) {	
			if(i == 0) {
				query += " COALESCE(c.r" + (rangos.get(i) - 1) + ", 0) AS `r" + (rangos.get(i) - 1) + "`, ";
			}else if (i+1 == rangos.size()) {
				query += " COALESCE(c.r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + ", 0) AS `r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "`, ";
				query += " COALESCE(c.r" + rangos.get(i) + ", 0) AS `r" + rangos.get(i) + "` ";
			}else {
				query += " COALESCE(c.r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + ", 0) AS `r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "`, ";
			}
		}
		query  += "FROM opciones_banos o "
				+ "LEFT JOIN conteo_datos c "
				+ "ON o.loc_estado = c.loc_estado "
				+ "   AND o.zona = c.zona "
				+ "   AND o.opcion_banos = c.banos_cantidad "
				+ "ORDER BY o.loc_estado, o.zona, o.opcion_banos;";
		
		
		
		return executeQueryRangos(query, headersTabla);
}
    
    public List<ResumenViviendaResultEntity> consultarResumenTipos(	String estado,
														     		String tipo,
																	String zona,
																	String segmento,
																	String precioFichaIni,
																	String precioFichaFin,
																	String preciom2Ini,
																	String preciom2Fin,
																	String periodoScrap,
														            String latBusc,
														            String lngBusc,
														            String kmBusc) {
		String query = "SELECT "
					+ "    loc_estado, "
					+ "    zona, "
					+ "    segmento, "
					+ "    COUNT(*) AS countRegistros, "
					+ "    ROUND(AVG(ficha_precio), 2) AS avgFichaPrecio, "
					+ "	   ROUND(MIN(ficha_precio), 2) AS minFichaPrecio,"
					+ "    ROUND(MAX(ficha_precio), 2) AS maxFichaPrecio,"
					+ "    ROUND(AVG(precio_m2), 2) AS avgPrecioM2, "
					+ "    ROUND(MIN(precio_m2), 2) AS minPrecioM2, "
					+ "    ROUND(MAX(precio_m2), 2) AS maxPrecioM2, "
					+ "    ROUND(AVG(superficie_cubierta), 2) AS avgSuperficieCubierta, "
					+ "    ROUND(AVG(dormitorios), 2) AS avgDormitorios, "
					+ "    ROUND(AVG(banos), 2) AS avgBanos, "
					+ "    ROUND(AVG(garages), 2) AS avgGarages "
					+ " FROM ( "
					+ "    SELECT "
					+ "        loc_estado, "
					+ "        zona, "
					+ "        segmento, "
					+ "        ficha_precio, "
					+ "        CASE "
					+ "            WHEN superficie_cubierta IS NULL THEN NULL "
					+ "            WHEN superficie_cubierta = 0 THEN NULL "
					+ "            ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) "
					+ "        END AS precio_m2, "
					+ "        superficie_cubierta, "
					+ "        dormitorios, "
					+ "        banos, "
					+ "        garages "
					+ "    FROM `fuentes_secundarias.online_listings_v1` "
					+ "   WHERE estatus IN ('entrante', 'existente') ";
		
		if(!periodoScrap.isEmpty()) {
			periodoScrap = periodoScrap.replaceAll(",","','");
			query += " AND periodo IN ('" + periodoScrap + "')";
		}
		
		if(!estado.isEmpty()) {
			estado = estado.replaceAll(",","','");
			query += " AND loc_estado IN ('" + estado + "')";
		}
		
		if(!zona.isEmpty()) {
			zona = zona.replaceAll(",","','");
			query += " AND zona IN ('" + zona + "')";
		}
		
		if(!segmento.isEmpty()) {
			segmento = segmento.replaceAll(",","','");
			query += " AND segmento IN ('" + segmento + "')";
		}
		
		if(!precioFichaIni.isEmpty()) {
			query += " AND ficha_precio >= " + precioFichaIni;
		}
		
		if(!precioFichaFin.isEmpty()) {
			query += " AND ficha_precio <= " + precioFichaFin;
		}
		
		if(!preciom2Ini.isEmpty()) {
			query += " AND CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END >= " + preciom2Ini;
		}
		
		if(!preciom2Fin.isEmpty()) {
			query += " AND CASE WHEN superficie_cubierta IS NULL THEN NULL WHEN superficie_cubierta = 0 THEN NULL ELSE ROUND(ficha_precio / CAST(superficie_cubierta AS FLOAT64), 4) END <= " + preciom2Fin;
		}
		
		if(!latBusc.isEmpty() && !lngBusc.isEmpty()) {
        	query += " AND lat <= (" + latBusc + " + " + kmBusc + ") AND lat >= (" + latBusc + " - " + kmBusc + ") "
        			+ " AND lng <= (" + lngBusc + " + " + kmBusc + ") AND lng >= (" + lngBusc + " - " + kmBusc + ") ";
        }
		
		if(tipo.equals("VV"))
			query += " AND loc_tipo IN ('Departamento','Departamento','Departamentos','Departamentos en Venta') ";
		if(tipo.equals("VH"))
			query += " AND loc_tipo IN ('Casas') ";
		if(tipo.equals("L"))
			query += " AND loc_tipo IN ('Locales Comerciales') ";
		if(tipo.equals("O"))
			query += " AND loc_tipo IN ('Oficinas') ";
		if(tipo.equals("B"))
			query += " AND loc_tipo IN ('Bodegas') ";
		if(tipo.equals("S"))
			query += " AND loc_tipo IN ('...', 'N/A','Venta') ";
		if(tipo.equals("X"))
			query += " AND loc_tipo IN ('EVITAR MOSTRAR') ";
		
		
		query 	+= " ) "
				+ " GROUP BY "
				+ "    loc_estado, zona, segmento "
				+ " ORDER BY "
				+ "    loc_estado, zona, segmento; ";
		
		return executeQueryResumenVivienda(query);
	}

    private List<BigQueryResulteEntity> executeQueryBigData(String query) {
        List<BigQueryResulteEntity> results = new ArrayList<>();

        try (InputStream credentialsStream = BigQueryService.class.getClassLoader().getResourceAsStream(CREDENTIALS_PATH)) {
            if (credentialsStream == null) {
                throw new IOException("No se encontró el archivo de credenciales en la ruta especificada: " + CREDENTIALS_PATH);
            }

            // Cargar las credenciales
            GoogleCredentials credentials = GoogleCredentials.fromStream(credentialsStream);

            // Inicializar el cliente de BigQuery
            BigQuery bigQuery = BigQueryOptions.newBuilder()
                    .setCredentials(credentials)
                    .build()
                    .getService();

            // Configurar la consulta
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            // Ejecutar la consulta y procesar los resultados
            TableResult queryResults = bigQuery.query(queryConfig);
            System.out.println("Consulta ejecutada executeQueryBigData: " + query);
            System.out.println("Filas devueltas: " + queryResults.getTotalRows());
        	System.out.println("-----------------------");
            
            queryResults.iterateAll().forEach(row -> {
            	//System.out.println(row.toString()); // Inspecciona los valores devueltos por la consulta (Comentar si no se esta en debug)
                BigQueryResulteEntity entity = new BigQueryResulteEntity();
                entity.setIdConsulta(row.get("idConsulta") != null && !row.get("idConsulta").isNull() ? row.get("idConsulta").getStringValue() : null);
                entity.setUrl(row.get("url") != null && !row.get("url").isNull() ? row.get("url").getStringValue() : null);
                entity.setDiasPub(row.get("dias_pub") != null && !row.get("dias_pub").isNull() ? row.get("dias_pub").getStringValue() : null);
                entity.setAntiguedad(row.get("antiguedad") != null && !row.get("antiguedad").isNull() ? row.get("antiguedad").getStringValue() : null);
                entity.setBanos(row.get("banos") != null && !row.get("banos").isNull() ? row.get("banos").getDoubleValue() : null);
                entity.setDescripcion(row.get("descripcion") != null && !row.get("descripcion").isNull() ? row.get("descripcion").getStringValue() : null);
                entity.setDormitorios(row.get("dormitorios") != null && !row.get("dormitorios").isNull() ? row.get("dormitorios").getDoubleValue() : null);
                entity.setFechaScrap(row.get("fecha_scrap") != null && !row.get("fecha_scrap").isNull() ? row.get("fecha_scrap").getStringValue() : null);
                entity.setFichaPrecio(row.get("ficha_precio") != null && !row.get("ficha_precio").isNull() ? row.get("ficha_precio").getDoubleValue() : null);
                entity.setFuente(row.get("fuente") != null && !row.get("fuente").isNull() ? row.get("fuente").getStringValue() : null);
                entity.setGarages(row.get("garages") != null && !row.get("garages").isNull() ? row.get("garages").getDoubleValue() : null);
                entity.setLat(row.get("lat") != null && !row.get("lat").isNull() ? row.get("lat").getDoubleValue() : null);
                entity.setLng(row.get("lng") != null && !row.get("lng").isNull() ? row.get("lng").getDoubleValue() : null);
                entity.setLocCiudad(row.get("loc_ciudad") != null && !row.get("loc_ciudad").isNull() ? row.get("loc_ciudad").getStringValue() : null);
                entity.setLocDireccion(row.get("loc_direccion") != null && !row.get("loc_direccion").isNull() ? row.get("loc_direccion").getStringValue() : null);
                entity.setLocEstado(row.get("loc_estado") != null && !row.get("loc_estado").isNull() ? row.get("loc_estado").getStringValue() : null);
                entity.setLocTipo(row.get("loc_tipo") != null && !row.get("loc_tipo").isNull() ? row.get("loc_tipo").getStringValue() : null);
                entity.setLocZona(row.get("loc_zona") != null && !row.get("loc_zona").isNull() ? row.get("loc_zona").getStringValue() : null);
                entity.setMonedaPub(row.get("moneda_pub") != null && !row.get("moneda_pub").isNull() ? row.get("moneda_pub").getStringValue() : null);
                entity.setNombre(row.get("nombre") != null && !row.get("nombre").isNull() ? row.get("nombre").getStringValue() : null);
                entity.setPrecioPub(row.get("precio_pub") != null && !row.get("precio_pub").isNull() ? row.get("precio_pub").getDoubleValue() : null);
                entity.setSegmento(row.get("segmento") != null && !row.get("segmento").isNull() ? row.get("segmento").getStringValue() : null);
                entity.setSuperficieCubierta(row.get("superficie_cubierta") != null && !row.get("superficie_cubierta").isNull() ? row.get("superficie_cubierta").getDoubleValue() : null);
                entity.setSuperficieTotal(row.get("superficie_total") != null && !row.get("superficie_total").isNull() ? row.get("superficie_total").getDoubleValue() : null);
                entity.setWebscrapId(row.get("webscrap_id") != null && !row.get("webscrap_id").isNull() ? row.get("webscrap_id").getStringValue() : null);
                entity.setCVEGEO(row.get("cvegeo") != null && !row.get("cvegeo").isNull() ? row.get("cvegeo").getStringValue() : null);
                entity.setZona(row.get("zona") != null && !row.get("zona").isNull() ? row.get("zona").getStringValue() : null);
                entity.setPrecioM2(row.get("precio_m2") != null && !row.get("precio_m2").isNull() ? row.get("precio_m2").getDoubleValue() : null);

                results.add(entity);
            });

            System.out.println("Consulta ejecutada exitosamente.");
        } catch (Exception e) {
            System.err.println("Error al ejecutar la consulta: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }
    
    private List<CatalogResultEntity> executeQueryCatalogos(String query) {
        List<CatalogResultEntity> results = new ArrayList<>();

        try (InputStream credentialsStream = BigQueryService.class.getClassLoader().getResourceAsStream(CREDENTIALS_PATH)) {
            if (credentialsStream == null) {
                throw new IOException("No se encontró el archivo de credenciales en la ruta especificada: " + CREDENTIALS_PATH);
            }

            // Cargar las credenciales
            GoogleCredentials credentials = GoogleCredentials.fromStream(credentialsStream);

            // Inicializar el cliente de BigQuery
            BigQuery bigQuery = BigQueryOptions.newBuilder()
                    .setCredentials(credentials)
                    .build()
                    .getService();

            // Configurar la consulta
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            // Ejecutar la consulta y procesar los resultados
            TableResult queryResults = bigQuery.query(queryConfig);
            System.out.println("Consulta ejecutada CatalogResultEntity: " + query);
            System.out.println("Filas devueltas: " + queryResults.getTotalRows());
        	System.out.println("-----------------------");
            
            queryResults.iterateAll().forEach(row -> {
            	CatalogResultEntity entity = new CatalogResultEntity();
                entity.setCatalogName(row.get("catalogName") != null && !row.get("catalogName").isNull() ? row.get("catalogName").getStringValue() : null);  
                
                results.add(entity);
            });

            System.out.println("Consulta ejecutada exitosamente.");
        } catch (Exception e) {
            System.err.println("Error al ejecutar la consulta: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }
    
    
    
    private JSONObject executeQueryRangos(String query, ArrayList<String> headersTabla) {
        List<CatalogResultEntity> results = new ArrayList<>();
        String[] stringHeaders = headersTabla.toArray(new String[0]);
        JSONObject jsonResult = null;

        try (InputStream credentialsStream = BigQueryService.class.getClassLoader().getResourceAsStream(CREDENTIALS_PATH)) {
            if (credentialsStream == null) {
                throw new IOException("No se encontró el archivo de credenciales en la ruta especificada: " + CREDENTIALS_PATH);
            }

            // Cargar las credenciales
            GoogleCredentials credentials = GoogleCredentials.fromStream(credentialsStream);

            // Inicializar el cliente de BigQuery
            BigQuery bigQuery = BigQueryOptions.newBuilder()
                    .setCredentials(credentials)
                    .build()
                    .getService();

            // Configurar la consulta
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            // Ejecutar la consulta y procesar los resultados
            TableResult queryResults = bigQuery.query(queryConfig);
            System.out.println("Consulta ejecutada CatalogResultEntity: " + query);
            System.out.println("Filas devueltas: " + queryResults.getTotalRows());
        	System.out.println("-----------------------");
            
        	// Convertir los resultados a JSON
            jsonResult = processBigQueryResults(queryResults);

            System.out.println("Consulta ejecutada exitosamente.");
        } catch (Exception e) {
            System.err.println("Error al ejecutar la consulta: " + e.getMessage());
            e.printStackTrace();
        }

        return jsonResult;
    }
    
    private List<ResumenViviendaResultEntity> executeQueryResumenVivienda(String query) {
        List<ResumenViviendaResultEntity> results = new ArrayList<>();

        try (InputStream credentialsStream = BigQueryService.class.getClassLoader().getResourceAsStream(CREDENTIALS_PATH)) {
            if (credentialsStream == null) {
                throw new IOException("No se encontró el archivo de credenciales en la ruta especificada: " + CREDENTIALS_PATH);
            }

            // Cargar las credenciales
            GoogleCredentials credentials = GoogleCredentials.fromStream(credentialsStream);

            // Inicializar el cliente de BigQuery
            BigQuery bigQuery = BigQueryOptions.newBuilder()
                    .setCredentials(credentials)
                    .build()
                    .getService();

            // Configurar la consulta
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            // Ejecutar la consulta y procesar los resultados
            TableResult queryResults = bigQuery.query(queryConfig);
            System.out.println("Consulta ejecutada executeQueryBigData: " + query);
            System.out.println("Filas devueltas: " + queryResults.getTotalRows());
        	System.out.println("-----------------------");
            
            queryResults.iterateAll().forEach(row -> {
            	ResumenViviendaResultEntity entity = new ResumenViviendaResultEntity();
                entity.setLocEstado(row.get("loc_estado") != null && !row.get("loc_estado").isNull() ? row.get("loc_estado").getStringValue() : null);
                entity.setZona(row.get("zona") != null && !row.get("zona").isNull() ? row.get("zona").getStringValue() : null);
                entity.setSegmento(row.get("segmento") != null && !row.get("segmento").isNull() ? row.get("segmento").getStringValue() : null);
                entity.setCountRegistros(row.get("countRegistros") != null && !row.get("countRegistros").isNull() ? row.get("countRegistros").getStringValue() : null);
                entity.setAvgFichaPrecio(row.get("avgFichaPrecio") != null && !row.get("avgFichaPrecio").isNull() ? row.get("avgFichaPrecio").getStringValue() : null);
                entity.setMinFichaPrecio(row.get("minFichaPrecio") != null && !row.get("minFichaPrecio").isNull() ? row.get("minFichaPrecio").getStringValue() : null);
                entity.setMaxFichaPrecio(row.get("maxFichaPrecio") != null && !row.get("maxFichaPrecio").isNull() ? row.get("maxFichaPrecio").getStringValue() : null);
                entity.setAvgPrecioM2(row.get("avgPrecioM2") != null && !row.get("avgPrecioM2").isNull() ? row.get("avgPrecioM2").getStringValue() : null);
                entity.setMinPrecioM2(row.get("minPrecioM2") != null && !row.get("minPrecioM2").isNull() ? row.get("minPrecioM2").getStringValue() : null);
                entity.setMaxPrecioM2(row.get("maxPrecioM2") != null && !row.get("maxPrecioM2").isNull() ? row.get("maxPrecioM2").getStringValue() : null);
                entity.setAvgSuperficieCubierta(row.get("avgSuperficieCubierta") != null && !row.get("avgSuperficieCubierta").isNull() ? row.get("avgSuperficieCubierta").getStringValue() : null);
                entity.setAvgDormitorios(row.get("avgDormitorios") != null && !row.get("avgDormitorios").isNull() ? row.get("avgDormitorios").getStringValue() : null);
                entity.setAvgBanos(row.get("avgBanos") != null && !row.get("avgBanos").isNull() ? row.get("avgBanos").getStringValue() : null);
                entity.setAvgGarages(row.get("avgGarages") != null && !row.get("avgGarages").isNull() ? row.get("avgGarages").getStringValue() : null);
                
                results.add(entity);
            });

            System.out.println("Consulta ejecutada exitosamente.");
        } catch (Exception e) {
            System.err.println("Error al ejecutar la consulta: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }
    
    public static List<Integer> dividirXMontos(int monto1, int monto2, int divisor) {
        List<Integer> resultados = new ArrayList<>();
        for (int i = monto1; i <= monto2; i += divisor) {
            resultados.add(i);
        }
        return resultados;
    }
    
    public static JSONObject processBigQueryResults(TableResult queryResults) {
        // Obtener los encabezados de la tabla
        List<String> headers = new ArrayList<>();
        for (Field field : queryResults.getSchema().getFields()) {
            headers.add(field.getName());
        }

        // Estructura para guardar los datos
        Map<String, List<JSONObject>> groupedData = new LinkedHashMap<>();

        // Iterar sobre las filas de la consulta
        for (FieldValueList row : queryResults.iterateAll()) {
            // Obtener el valor de la primera columna como clave (e.g., "loc_estado")
            String locEstado = row.get("loc_estado").getStringValue();

            // Crear un objeto JSON para la fila actual
            JSONObject rowJson = new JSONObject();

            for (String header : headers) {
                FieldValue value = row.get(header);
                rowJson.put(header, value.isNull() ? "-" : value.getStringValue());
            }

            // Agrupar las filas por loc_estado
            groupedData.computeIfAbsent(locEstado, k -> new ArrayList<>()).add(rowJson);
        }
        
        //Convertir la estructura a JSON
        JSONObject result = new JSONObject();
        for (Map.Entry<String, List<JSONObject>> entry : groupedData.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    public static JSONObject convertToDynamicJson(List<String> headers, Map<String, List<String>> data) {
        JSONObject result = new JSONObject();

        // Ajustar los encabezados: Excluir la primera columna (clave)
        List<String> adjustedHeaders = headers.subList(1, headers.size());

        // Recorrer cada fila
        for (Map.Entry<String, List<String>> entry : data.entrySet()) {
            String rowName = entry.getKey();
            List<String> rowValues = entry.getValue();

            // Crear un sub-JSON para cada fila
            JSONObject rowJson = new JSONObject();

            for (int i = 0; i < adjustedHeaders.size(); i++) {
                rowJson.put(adjustedHeaders.get(i), rowValues.get(i));
            }

            // Añadir al resultado
            result.put(rowName, rowJson);
        }

        return result;
    }
}