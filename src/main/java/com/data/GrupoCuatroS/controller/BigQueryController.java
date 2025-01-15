package com.data.GrupoCuatroS.controller;

import java.util.List;
import java.util.Optional;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.data.GrupoCuatroS.entity.BigQueryResulteEntity;
import com.data.GrupoCuatroS.entity.CatalogResultEntity;
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
            @RequestParam String fechaIni,
            @RequestParam String fechaFin
    ) {
        // Llama al servicio para ejecutar la consulta con filtros
        try {
			return bigQueryService.consultarConFiltros(estado, tipo, zona, segmento, precioFichaIni, precioFichaFin, preciom2Ini, preciom2Fin, fechaIni, fechaFin);
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
    
    @GetMapping("/getTipo")
    public List<CatalogResultEntity> getTipoVivienda() {
        // Llama al servicio para ejecutar la consulta de Catalogo Tipo Vivienda
        try {
			return bigQueryService.getTipoVivienda();
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
			return bigQueryService.getZona();
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
    
    @GetMapping("/getDormitoriosRango")
    public ResponseEntity<Object>  getDormitoriosPrecio(
			@RequestParam String estado,
		    @RequestParam String tipo,
		    @RequestParam String zona,
		    @RequestParam String segmento,
		    @RequestParam String precioFichaIni,
		    @RequestParam String precioFichaFin,
		    @RequestParam String preciom2Ini,
		    @RequestParam String preciom2Fin,
		    @RequestParam String fechaIni,
		    @RequestParam String fechaFin,
		    @RequestParam String rangosPrecio,
		    @RequestParam String indicadorMonto) {
        // Llama al servicio para ejecutar la consulta de Dormitorios por rangos
        try {
        	//si el indicadorMonto = 1 buscara por PrecioFicha
        	//si el indicadorMonto = 0 buscara por Preciom2
			JSONObject result = bigQueryService.getDormitoriosPrecio(
	                estado, tipo, zona, segmento, precioFichaIni,
	                precioFichaFin, preciom2Ini, preciom2Fin, 
	                fechaIni, fechaFin, rangosPrecio, indicadorMonto);

	        // Convierte el JSONObject a un objeto que Spring pueda serializar
	        return ResponseEntity.ok(result.toMap());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }

}
