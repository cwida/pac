-- TPC-H PAC Links Configuration
-- This file adds PAC_LINK metadata to existing TPC-H tables
-- Execute this after the tables are created and loaded with data
-- PAC_LINKs define foreign key relationships for PAC compilation

-- Mark customer as the privacy unit
ALTER TABLE customer ADD PAC_KEY (c_custkey);
ALTER TABLE customer SET PU;

-- Protected columns in customer table
ALTER PU TABLE customer ADD PROTECTED (c_custkey);
ALTER PU TABLE customer ADD PROTECTED (c_comment);
ALTER PU TABLE customer ADD PROTECTED (c_acctbal);
ALTER PU TABLE customer ADD PROTECTED (c_name);
ALTER PU TABLE customer ADD PROTECTED (c_address);

-- Orders -> Customer and Lineitem->Orders links
ALTER TABLE orders ADD PAC_LINK (o_custkey) REFERENCES customer(c_custkey);
ALTER TABLE lineitem ADD PAC_LINK (l_orderkey) REFERENCES orders(o_orderkey);

-- Protect the comment columns, as they may include customer-specific notes
ALTER TABLE orders ADD PROTECTED (o_comment);
ALTER TABLE lineitem ADD PROTECTED (l_comment);
