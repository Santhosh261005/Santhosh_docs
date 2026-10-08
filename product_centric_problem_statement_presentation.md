# Product Performance & Inventory Optimization Intelligence

## Problem Statement

E-commerce businesses with large product catalogs face two major inventory challenges:

- **Stockouts:** High-demand products may run out of inventory, leading to potential lost sales.
- **Overstock:** Low-demand products may hold excess inventory, increasing storage costs and locking working capital.

The required information is distributed across **product, sales, inventory, procurement, and supplier data**, making it difficult to understand the overall performance and inventory health of each product.

## Proposed Solution

Build a **product-centric data engineering and analytics platform** that integrates these datasets and converts raw operational data into meaningful product-level insights.

The platform will help answer:

> **Which products are important to the business, how fast are they selling, how long will the available stock last, and which products require inventory action?**

## Key KPIs

### 1. Sales Velocity
Measures how quickly a product is selling.

`Sales Velocity = Units Sold / Number of Days`

### 2. Days of Stock Cover
Estimates how many days the available inventory can support the current sales rate.

`Days of Stock Cover = Available Stock / Sales Velocity`

### 3. ABC Product Classification
Classifies products based on their contribution to overall sales revenue:

- **A:** High business importance
- **B:** Medium business importance
- **C:** Lower business importance / long-tail products

### 4. Inventory Health Classification
Combines product sales and inventory information to classify each product as:

- **Stockout Risk** - insufficient inventory for the current sales rate
- **Healthy** - inventory is appropriately balanced
- **Overstock** - excessive inventory compared with sales
- **Non-Moving** - inventory exists but the product has no sales during the analysis period

## Expected Business Outcome

```text
Product Data + Sales + Inventory + Procurement
                    |
                    v
              Product KPIs
                    |
                    v
         Inventory Health Analysis
                    |
                    v
     Replenish | Monitor | Reduce Stock
```

The final goal is to move beyond a traditional sales dashboard and create a **product-level decision-support system** that helps the business maintain product availability while reducing unnecessary inventory.

## One-Line Capstone Summary

> **Build a Product Performance & Inventory Optimization Intelligence Platform that identifies revenue-critical products, measures product sales velocity and stock coverage, and detects stockout, healthy, overstock, and non-moving inventory conditions to support better inventory decisions.**
