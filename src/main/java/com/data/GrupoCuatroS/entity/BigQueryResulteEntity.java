package com.data.GrupoCuatroS.entity;

import jakarta.persistence.Entity;

@Entity
public class BigQueryResulteEntity {

    private String idConsulta;
	private String url;
    private String diasPub;
    private String antiguedad;
    private Double banos;
    private String descripcion;
    private Double dormitorios;
    private String fechaScrap;
    private Double fichaPrecio;
    private String fuente;
    private Double garages;
    private Double lat;
    private Double lng;
    private String locCiudad;
    private String locDireccion;
    private String locEstado;
    private String locTipo;
    private String locZona;
    private String monedaPub;
    private String nombre;
    private Double precioPub;
    private String segmento;
    private Double superficieCubierta;
    private Double superficieTotal;
    private String webscrapId;
    private String CVEGEO;
    private String zona;
    private Double precioM2;

    // Getters y setters
	public Double getPrecioM2() {
		return precioM2;
	}
	public void setPrecioM2(Double precioM2) {
		this.precioM2 = precioM2;
	}
	public String getZona() {
		return zona;
	}
	public void setZona(String zona) {
		this.zona = zona;
	}
	public String getCVEGEO() {
		return CVEGEO;
	}
	public void setCVEGEO(String cVEGEO) {
		CVEGEO = cVEGEO;
	}
	public String getWebscrapId() {
		return webscrapId;
	}
	public void setWebscrapId(String webscrapId) {
		this.webscrapId = webscrapId;
	}
	public Double getSuperficieTotal() {
		return superficieTotal;
	}
	public void setSuperficieTotal(Double superficieTotal) {
		this.superficieTotal = superficieTotal;
	}
	public Double getSuperficieCubierta() {
		return superficieCubierta;
	}
	public void setSuperficieCubierta(Double superficieCubierta) {
		this.superficieCubierta = superficieCubierta;
	}
	public String getSegmento() {
		return segmento;
	}
	public void setSegmento(String segmento) {
		this.segmento = segmento;
	}
	public Double getPrecioPub() {
		return precioPub;
	}
	public void setPrecioPub(Double precioPub) {
		this.precioPub = precioPub;
	}
	public String getNombre() {
		return nombre;
	}
	public void setNombre(String nombre) {
		this.nombre = nombre;
	}
	public String getMonedaPub() {
		return monedaPub;
	}
	public void setMonedaPub(String monedaPub) {
		this.monedaPub = monedaPub;
	}
	public String getLocZona() {
		return locZona;
	}
	public void setLocZona(String locZona) {
		this.locZona = locZona;
	}
	public String getLocTipo() {
		return locTipo;
	}
	public void setLocTipo(String locTipo) {
		this.locTipo = locTipo;
	}
	public String getLocEstado() {
		return locEstado;
	}
	public void setLocEstado(String locEstado) {
		this.locEstado = locEstado;
	}
	public String getLocDireccion() {
		return locDireccion;
	}
	public void setLocDireccion(String locDireccion) {
		this.locDireccion = locDireccion;
	}
	public String getLocCiudad() {
		return locCiudad;
	}
	public void setLocCiudad(String locCiudad) {
		this.locCiudad = locCiudad;
	}
	public Double getLng() {
		return lng;
	}
	public void setLng(Double lng) {
		this.lng = lng;
	}
	public Double getLat() {
		return lat;
	}
	public void setLat(Double lat) {
		this.lat = lat;
	}
	public Double getGarages() {
		return garages;
	}
	public void setGarages(Double garages) {
		this.garages = garages;
	}
	public String getFuente() {
		return fuente;
	}
	public void setFuente(String fuente) {
		this.fuente = fuente;
	}
	public Double getFichaPrecio() {
		return fichaPrecio;
	}
	public void setFichaPrecio(Double fichaPrecio) {
		this.fichaPrecio = fichaPrecio;
	}
	public String getFechaScrap() {
		return fechaScrap;
	}
	public void setFechaScrap(String fechaScrap) {
		this.fechaScrap = fechaScrap;
	}
	public Double getDormitorios() {
		return dormitorios;
	}
	public void setDormitorios(Double dormitorios) {
		this.dormitorios = dormitorios;
	}
	public String getDescripcion() {
		return descripcion;
	}
	public void setDescripcion(String descripcion) {
		this.descripcion = descripcion;
	}
	public Double getBanos() {
		return banos;
	}
	public void setBanos(Double banos) {
		this.banos = banos;
	}
	public String getAntiguedad() {
		return antiguedad;
	}
	public void setAntiguedad(String antiguedad) {
		this.antiguedad = antiguedad;
	}
	public String getDiasPub() {
		return diasPub;
	}
	public void setDiasPub(String diasPub) {
		this.diasPub = diasPub;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getIdConsulta() {
		return idConsulta;
	}
	public void setIdConsulta(String idConsulta) {
		this.idConsulta = idConsulta;
	}
    
}
