CREATE TABLE "public"."products" (
"product_id" INTEGER IDENTITY(1,1) PRIMARY KEY,
"product_name" VARCHAR(255) UNIQUE,
"product_price" NUMERIC(10,2)
);

CREATE TABLE "public"."payments" (
"payment_id" INTEGER IDENTITY(1,1) PRIMARY KEY,
"payment_type" VARCHAR(255) UNIQUE
);

CREATE TABLE "public"."branches" (
"branch_id" INTEGER IDENTITY(1,1) PRIMARY KEY,
"branch_name" VARCHAR(255) UNIQUE
);

CREATE TABLE "public"."transactions" (
"transaction_id" INTEGER IDENTITY(1,1) PRIMARY KEY,
"branch_id" INTEGER,
"payment_type_id" INTEGER,
"total_price" NUMERIC(10,2),
"date_time" TIMESTAMP
);

CREATE TABLE "public"."baskets" (
"basket_item_id" INTEGER IDENTITY(1,1) PRIMARY KEY,
"transaction_id" INTEGER,
"product_id" INTEGER
);

ALTER TABLE transactions
ADD CONSTRAINT fk_transaction_branch 
FOREIGN KEY (branch_id) 
REFERENCES branches(branch_id);

ALTER TABLE transactions
ADD CONSTRAINT fk_transaction_payment 
FOREIGN KEY (payment_type_id) 
REFERENCES payments(payment_id);

ALTER TABLE baskets
ADD CONSTRAINT fk_basket_transaction 
FOREIGN KEY (transaction_id) 
REFERENCES transactions(transaction_id);

ALTER TABLE baskets
ADD CONSTRAINT fk_basket_product 
FOREIGN KEY (product_id) 
REFERENCES products(product_id);