-- TPC-H PAC Links Configuration
-- This file adds PAC_LINK metadata to existing TPC-H tables
-- Execute this after the tables are created and loaded with data
-- PAC_LINKs define foreign key relationships for PAC compilation

-- Mark customer as the privacy unit
ALTER TABLE customer SET PU;

-- Protected columns in customer table
ALTER PU TABLE customer ADD PROTECTED (c_custkey);
ALTER PU TABLE customer ADD PROTECTED (c_comment);
ALTER PU TABLE customer ADD PROTECTED (c_acctbal);
ALTER PU TABLE customer ADD PROTECTED (c_name);
ALTER PU TABLE customer ADD PROTECTED (c_address);

-- Orders -> Customer link
ALTER PU TABLE orders ADD PAC_LINK (o_custkey) REFERENCES customer(c_custkey);

-- Lineitem -> Orders link
ALTER PU TABLE lineitem ADD PAC_LINK (l_orderkey) REFERENCES orders(o_orderkey);
