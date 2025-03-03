package com.data.GrupoCuatroS.controller;

import java.util.List;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.data.GrupoCuatroS.entity.BigQueryResulteEntity;
import com.data.GrupoCuatroS.entity.CatalogResultEntity;
import com.data.GrupoCuatroS.entity.ResumenViviendaResultEntity;
import com.data.GrupoCuatroS.service.BigQueryService;

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

}
