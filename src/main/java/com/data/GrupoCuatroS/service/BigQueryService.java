package com.data.GrupoCuatroS.service;

import com.data.GrupoCuatroS.entity.BigQueryResulteEntity;
import com.data.GrupoCuatroS.entity.CatalogResultEntity;
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
												            String fechaIni,
												            String fechaFin) {
        // Consulta SQL construida dinámicamente
        String query =
                "SELECT url, "
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
        
        if(!estado.isEmpty() || !tipo.isEmpty() || !zona.isEmpty() || !segmento.isEmpty() || !precioFichaIni.isEmpty() || !precioFichaFin.isEmpty() || !preciom2Ini.isEmpty() || !preciom2Fin.isEmpty() || !fechaIni.isEmpty() || !fechaFin.isEmpty()) {
        	query += "  WHERE ";
        	
        	if(fechaIni != null && !fechaIni.isEmpty() && fechaFin != null && !fechaIni.isEmpty()) {
        		query += "  PARSE_DATE('%Y-%m-%d',  fecha_scrap||'-02') BETWEEN PARSE_DATE('%Y-%m-%d',  '"+ fechaIni +"-01') AND PARSE_DATE('%Y-%m-%d',  '"+ fechaFin +"-03')";
        	}
        	
        	if(!estado.isEmpty()) {
        		estado = estado.replaceAll(",","','");
        		query += " AND loc_estado IN ('" + estado + "')";
        	}
        	
        	if(!tipo.isEmpty()) {
        		tipo = tipo.replaceAll(",","','");
        		query += " AND loc_tipo IN ('" + tipo + "')";
        	}
        	
        	if(!zona.isEmpty()) {
        		zona = zona.replaceAll(",","','");
        		query += " AND loc_zona IN ('" + zona + "')";
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
        	
        	
        }
        
        //query += "  LIMIT 15";

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
    
    public List<CatalogResultEntity> getTipoVivienda() {
        // Consulta SQL construida dinámicamente
        String query =
                "SELECT DISTINCT loc_tipo AS catalogName"
                + "   FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` "
                + "  ORDER BY 1";

        return executeQueryCatalogos(query);
    }
    
    public List<CatalogResultEntity> getZona() {
        // Consulta SQL construida dinámicamente
        String query =
                "SELECT DISTINCT zona AS catalogName"
                + "   FROM `data-agregador-main.fuentes_secundarias.online_listings_v1` "
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
    
    public JSONObject getDormitoriosPrecio(String estado,
    										 String tipo,
								             String zona,
								             String segmento,
								             String precioFichaIni,
								             String precioFichaFin,
								             String preciom2Ini,
								             String preciom2Fin,
								             String fechaIni,
								             String fechaFin,
								             String rangosPrecio,
								             String indicador) {
        
    	List<Integer> rangos; 
    	
    	if(Integer.valueOf(indicador) == 1) {
    		rangos = dividirXMontos(Integer.valueOf(precioFichaIni), Integer.valueOf(precioFichaFin), Integer.valueOf(rangosPrecio));
    	}else {
    		rangos = dividirXMontos(Integer.valueOf(preciom2Ini), Integer.valueOf(preciom2Fin), Integer.valueOf(rangosPrecio));
    	}
    	
    	
    	ArrayList<String> headersTabla = new ArrayList<String>();
    	headersTabla.add("loc_estado");
    	headersTabla.add("loc_zona");
    	
    	//System.out.println("----------------------- RANGOS.SIZE()= " + rangos.size());
    	// Consulta SQL construida dinámicamente
        String query = "WITH datos_agrupados AS ( "
        		+ "  SELECT  "
        		+ "      loc_estado, "
        		+ "      loc_zona, "
        		+ "      CASE  "
        		+ "          WHEN dormitorios <= 2 THEN '2 dormitorios o menos' "
        		+ "          WHEN dormitorios = 3 THEN '3 dormitorios' "
        		+ "          WHEN dormitorios = 4 THEN '4 dormitorios' "
        		+ "          WHEN dormitorios > 4 THEN 'Mas de 4 dormitorios' "
        		+ "      END AS dormitorios_cantidad, "
        		+ "      CASE  ";
        
        
        for (int i = 0; i < rangos.size(); i++) {	
        	if(i == 0) {
        		headersTabla.add("r" + (rangos.get(i) - 1));
        		query += " WHEN ficha_precio < " + rangos.get(i) + " THEN 'r" + (rangos.get(i) - 1) + "' ";
        	}else if (i+1 == rangos.size()) {
        		headersTabla.add("r" + (rangos.get(i)));
        		query += " WHEN ficha_precio BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
        		query += " ELSE 'r" + rangos.get(i) + "'";
        	}else {
        		headersTabla.add("r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1));
        		query += " WHEN ficha_precio BETWEEN " + rangos.get(i-1) + " AND " + (rangos.get(i) - 1) + " THEN 'r" + rangos.get(i-1) + "_" + (rangos.get(i) - 1) + "' ";
        	}
        }
        
        query  += "      END AS rango_precio "
        		+ "  FROM  "
        		+ "      `data-agregador-main.fuentes_secundarias.online_listings_v1` ";
        
        if(!estado.isEmpty() || !tipo.isEmpty() || !zona.isEmpty() || !segmento.isEmpty() || !precioFichaIni.isEmpty() || !precioFichaFin.isEmpty() || !preciom2Ini.isEmpty() || !preciom2Fin.isEmpty() || !fechaIni.isEmpty() || !fechaFin.isEmpty()) {
        	query += "  WHERE ";
        	
        	if(fechaIni != null && !fechaIni.isEmpty() && fechaFin != null && !fechaIni.isEmpty()) {
        		query += "  PARSE_DATE('%Y-%m-%d',  fecha_scrap||'-02') BETWEEN PARSE_DATE('%Y-%m-%d',  '"+ fechaIni +"-01') AND PARSE_DATE('%Y-%m-%d',  '"+ fechaFin +"-03')";
        	}
        	
        	if(!estado.isEmpty()) {
        		estado = estado.replaceAll(",","','");
        		query += " AND loc_estado IN ('" + estado + "')";
        	}
        	
        	if(!tipo.isEmpty()) {
        		tipo = tipo.replaceAll(",","','");
        		query += " AND loc_tipo IN ('" + tipo + "')";
        	}
        	
        	if(!zona.isEmpty()) {
        		zona = zona.replaceAll(",","','");
        		query += " AND loc_zona IN ('" + zona + "')";
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
        
        }
        
        query  += ") "
        		+ " SELECT  "
        		+ "    loc_estado, "
        		+ "    loc_zona, "
        		+ "    dormitorios_cantidad, ";
        
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
        query  += " FROM  "
        		+ "    datos_agrupados "
        		+ " GROUP BY  "
        		+ "    loc_estado, loc_zona, dormitorios_cantidad "
        		+ " ORDER BY  "
        		+ "    loc_estado, loc_zona, dormitorios_cantidad;";
        
        
        

        return executeQueryRangos(query, headersTabla);
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