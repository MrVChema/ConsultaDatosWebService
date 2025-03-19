package com.data.GrupoCuatroS.controller;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.data.GrupoCuatroS.entity.BigQueryResulteEntity;
import com.data.GrupoCuatroS.entity.CatalogResultEntity;
import com.data.GrupoCuatroS.entity.ResumenViviendaResultEntity;
import com.data.GrupoCuatroS.service.BigQueryService;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
@RequestMapping("/api")
public class BigQueryController {
	@Autowired
    private BigQueryService bigQueryService; // Servicio que manejará la lógica

    // Consulta con parámetros opcionales enviados desde la petición
    @GetMapping("/consultar")
    public List<BigQueryResulteEntity> consultar(
            @RequestParam String estado,
            @RequestParam String tipo,
            @RequestParam String zona,
            @RequestParam String segmento,
            @RequestParam String precioFichaIni,
            @RequestParam String precioFichaFin,
            @RequestParam String preciom2Ini,
            @RequestParam String preciom2Fin,
            @RequestParam String periodoScrap,
            @RequestParam String latBusc,
            @RequestParam String lngBusc,
            @RequestParam String kmBusc
    ) {
        // Llama al servicio para ejecutar la consulta con filtros
        try {
			return bigQueryService.consultarConFiltros(estado, tipo, zona, segmento, precioFichaIni, precioFichaFin, preciom2Ini, preciom2Fin, periodoScrap, 0, latBusc, lngBusc, kmBusc);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }
    
    @GetMapping("/consultarSaliente")
    public List<BigQueryResulteEntity> consultarSaliente(
            @RequestParam String estado,
            @RequestParam String tipo,
            @RequestParam String zona,
            @RequestParam String segmento,
            @RequestParam String precioFichaIni,
            @RequestParam String precioFichaFin,
            @RequestParam String preciom2Ini,
            @RequestParam String preciom2Fin,
            @RequestParam String periodoScrap,
            @RequestParam String latBusc,
            @RequestParam String lngBusc,
            @RequestParam String kmBusc
    ) {
        // Llama al servicio para ejecutar la consulta con filtros
        try {
			return bigQueryService.consultarConFiltros(estado, tipo, zona, segmento, precioFichaIni, precioFichaFin, preciom2Ini, preciom2Fin, periodoScrap, 1, latBusc, lngBusc, kmBusc);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }
    
    @GetMapping("/getEstados")
    public List<CatalogResultEntity> getEstados() {
        // Llama al servicio para ejecutar la consulta de Catalogo estados
        try {
			return bigQueryService.getEstados();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }
    
    @GetMapping("/getZona")
    public List<CatalogResultEntity> getZona(@RequestParam String estado) {
        // Llama al servicio para ejecutar la consulta de Catalogo Zona
        try {
			return bigQueryService.getZona(estado);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }
    
    @GetMapping("/getSegmento")
    public List<CatalogResultEntity> getSegmento() {
        // Llama al servicio para ejecutar la consulta de Catalogo Segmento
        try {
			return bigQueryService.getSegmento();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }
    
    @GetMapping("/getPeriodo")
    public List<CatalogResultEntity> getPeriodo() {
        // Llama al servicio para ejecutar la consulta de Catalogo Periodos
        try {
			return bigQueryService.getPeriodo();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }
    
    @GetMapping("/getDormitoriosRangos")
    public ResponseEntity<Object>  getDormitoriosRangos(
			@RequestParam String estado,
		    @RequestParam String tipo,
		    @RequestParam String zona,
		    @RequestParam String segmento,
		    @RequestParam String iniBusc,
		    @RequestParam String finBusc,
		    @RequestParam String periodoScrap,
		    @RequestParam String rangosPrecio,
		    @RequestParam String indicadorMonto,
            @RequestParam String latBusc,
            @RequestParam String lngBusc,
            @RequestParam String kmBusc) {
        // Llama al servicio para ejecutar la consulta de Dormitorios por rangos
        try {
        	//si el indicadorMonto = 1 buscara por PrecioFicha
        	//si el indicadorMonto = 2 buscara por Preciom2
        	//si el indicadorMonto = 3 buscara por Superficie
        	//si el indicadorMonto = 0 regresara 0 registros para ocultar el resultado
			JSONObject result = bigQueryService.getDormitoriosRangos(
	                estado, tipo, zona, segmento, iniBusc,
	                finBusc, periodoScrap, rangosPrecio, indicadorMonto, latBusc, lngBusc, kmBusc);

	        // Convierte el JSONObject a un objeto que Spring pueda serializar
	        return ResponseEntity.ok(result.toMap());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }
    
    @GetMapping("/getBaniosRangos")
    public ResponseEntity<Object>  getBaniosRangos(
			@RequestParam String estado,
		    @RequestParam String tipo,
		    @RequestParam String zona,
		    @RequestParam String segmento,
		    @RequestParam String iniBusc,
		    @RequestParam String finBusc,
		    @RequestParam String periodoScrap,
		    @RequestParam String rangosPrecio,
		    @RequestParam String indicadorMonto,
            @RequestParam String latBusc,
            @RequestParam String lngBusc,
            @RequestParam String kmBusc) {
        // Llama al servicio para ejecutar la consulta de Dormitorios por rangos
        try {
        	//si el indicadorMonto = 1 buscara por PrecioFicha
        	//si el indicadorMonto = 2 buscara por Preciom2
        	//si el indicadorMonto = 3 buscara por Superficie
        	//si el indicadorMonto = 0 regresara 0 registros para ocultar el resultado
			JSONObject result = bigQueryService.getBaniosRangos(
	                estado, tipo, zona, segmento, iniBusc,
	                finBusc, periodoScrap, rangosPrecio, indicadorMonto, latBusc, lngBusc, kmBusc);

	        // Convierte el JSONObject a un objeto que Spring pueda serializar
	        return ResponseEntity.ok(result.toMap());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }
    
    @GetMapping("/consultarDetalleTipos")
    public List<ResumenViviendaResultEntity> consultarResumenTipos(
            @RequestParam String estado,
            @RequestParam String tipo,
            @RequestParam String zona,
            @RequestParam String segmento,
            @RequestParam String precioFichaIni,
            @RequestParam String precioFichaFin,
            @RequestParam String preciom2Ini,
            @RequestParam String preciom2Fin,
            @RequestParam String periodoScrap,
            @RequestParam String latBusc,
            @RequestParam String lngBusc,
            @RequestParam String kmBusc
    ) {
        // Llama al servicio para ejecutar la consultarResumenVivienda con filtros
        try {
        	List<ResumenViviendaResultEntity> result = bigQueryService.consultarResumenTipos(estado, tipo, zona, segmento, precioFichaIni, precioFichaFin, preciom2Ini, preciom2Fin, periodoScrap, latBusc, lngBusc, kmBusc);
        	return result;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }
    
    @PostMapping("/cargaMasiva1")
    public ResponseEntity<String> recibirCarga1(@RequestBody List<Map<String, String>> datos) {
        int countInserts = 0;
        int countError = 0;
        List<Map<String, String>> registrosConError = new ArrayList<>();

        try {
            // Primero ejecutamos el proceso de eliminación
            String resultDelete = bigQueryService.procesoDelete();
            System.out.println("Registros eliminados: " + resultDelete);

            // Procesamos cada registro que ya contiene la sentencia SQL
            for (Map<String, String> registro : datos) {
                try {
                    // Extraemos la sentencia SQL directamente del registro
                    String sentenciaSQL = registro.get("sql");
                    
                    if (sentenciaSQL != null && !sentenciaSQL.isEmpty()) {
                        // Enviamos la sentencia SQL al servicio
                        String resultado = bigQueryService.procesoInsert(sentenciaSQL);
                        
                        if ("Ok".equals(resultado)) {
                            countInserts++;
                        } else {
                            countError++;
                            // Guardamos el registro con error para reportarlo
                            Map<String, String> registroError = new HashMap<>(registro);
                            registroError.put("mensajeError", resultado);
                            registrosConError.add(registroError);
                        }
                    } else {
                        countError++;
                        Map<String, String> registroError = new HashMap<>(registro);
                        registroError.put("mensajeError", "Sentencia SQL vacía o nula");
                        registrosConError.add(registroError);
                    }
                } catch (Exception e) {
                    countError++;
                    // Guardamos el registro con error y su mensaje
                    Map<String, String> registroError = new HashMap<>(registro);
                    registroError.put("mensajeError", e.getMessage());
                    registrosConError.add(registroError);
                }
            }

            // Preparamos la respuesta
            StringBuilder response = new StringBuilder();
            response.append("Eliminacion de datos: ").append(resultDelete).append("\n");
            response.append("Registros en archivo: ").append(datos.size()).append("\n");
            response.append("Registros insertados correctamente: ").append(countInserts).append("\n");
            response.append("Registros con error: ").append(countError);

            // Si hay errores, podemos guardarlos para análisis posterior
            if (!registrosConError.isEmpty()) {
                guardarRegistrosConError(registrosConError);
            }

            return ResponseEntity.ok(response.toString());

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error en carga masiva: " + e.getMessage());
        }
    }
    
    @PostMapping("/cargaMasiva2")
    public String recibirCarga2(@RequestParam String estado) {
		String result = bigQueryService.procesoActualizaciones(estado);
	    System.out.println("Servicio de actualizacion: " + result);
	
	    return result;
    }

    /**
     * Guarda registros con error para su posterior análisis
     */
    private void guardarRegistrosConError(List<Map<String, String>> registrosConError) {
        try {
            // Guardar en un archivo JSON con timestamp para identificación única
            String nombreArchivo = "errores_carga_" + System.currentTimeMillis() + ".json";
            File archivo = new File(nombreArchivo);
            
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(archivo, registrosConError);
            
            System.out.println("Registros con error guardados en: " + archivo.getAbsolutePath());
        } catch (Exception e) {
            System.err.println("Error al guardar registros con error: " + e.getMessage());
        }
    }

}
