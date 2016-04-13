package com.xpatterns.jaws.data.contracts

import com.xpatterns.jaws.data.DTO.ParquetTable

trait TJawsParquetTables {
	def addParquetTable(pTable : ParquetTable, userId: String)
	def deleteParquetTable(name : String, userId: String)
	def listParquetTables(userId: String) : Array[ParquetTable]
	def tableExists(name : String, userId: String) : Boolean
	def readParquetTable(name : String, userId: String) : ParquetTable
}