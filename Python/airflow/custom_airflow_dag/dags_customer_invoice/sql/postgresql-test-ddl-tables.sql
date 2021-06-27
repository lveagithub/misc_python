-- Drop tables

DROP TABLE test.invoice;

DROP TABLE test.customer;

-- test.customer definition

CREATE TABLE IF NOT EXISTS test.customer (
	id int4 NOT NULL,
	customername varchar NULL,
	address varchar NULL,
	country varchar NULL,
	phone varchar NULL,
	CONSTRAINT customer_pk PRIMARY KEY (id)
);

-- test.invoice definition

CREATE TABLE IF NOT EXISTS test.invoice (
	id int4 NOT NULL,
	idcustomer int4 NULL,
	orderdate date NULL,
	subtotal float4 NULL,
	discount float4 NULL,
	region varchar NULL,
	salesperson varchar NULL,
	CONSTRAINT invoice_pk PRIMARY KEY (id),
	CONSTRAINT customer_fk FOREIGN KEY (id) REFERENCES test.customer(id)
);