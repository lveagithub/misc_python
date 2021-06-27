-- Enabling Local Data Loading Capability

SET GLOBAL local_infile=1;


-- Drop tables


DROP TABLE test.invoice;

DROP TABLE test.customer;


-- test.customer definition

CREATE TABLE IF NOT EXISTS `test`.`customer` (
  `id` int NOT NULL,
  `customerName` varchar(100) DEFAULT NULL,
  `address` varchar(100) DEFAULT NULL,
  `country` varchar(100) DEFAULT NULL,
  `phone` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- test.invoice definition

CREATE TABLE IF NOT EXISTS `test`.`invoice` (
  `id` varchar(100) NOT NULL,
  `idcustomer` int DEFAULT NULL,
  `orderdate` date DEFAULT NULL,
  `subtotal` float DEFAULT NULL,
  `discount` float DEFAULT NULL,
  `region` varchar(100) DEFAULT NULL,
  `salesperson` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `invoice_FK` (`idcustomer`),
  CONSTRAINT `invoice_FK` FOREIGN KEY (`idcustomer`) REFERENCES `customer` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;