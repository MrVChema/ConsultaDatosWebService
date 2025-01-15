package com.data.GrupoCuatroS.entity;

import jakarta.persistence.Entity;

@Entity
public class CatalogResultEntity {
	private String catalogName;

	public String getCatalogName() {
		return catalogName;
	}

	public void setCatalogName(String catalogName) {
		this.catalogName = catalogName;
	}

}
