# Product Performance & Inventory Optimization Intelligence

## 1. Capstone Overview

This capstone focuses on a **product-centric e-commerce analytics use case** using synthetic operational data generated from an Odoo-like platform. Instead of attempting to analyze the complete order-to-cash lifecycle, the project uses the **product as the central business entity** and combines sales, inventory, procurement, supplier, and product-master data to understand product performance and inventory health.

The objective is not simply to report which products sold the most. The platform should help the business determine:

- Which products are most important to revenue?
- How quickly is each product selling?
- How long will the current available inventory last?
- Which products are at risk of running out of stock?
- Which products have excessive or non-moving inventory?
- What inventory action should the business consider for each product?

---

## 2. Problem Statement

E-commerce businesses managing large product catalogs need to maintain a balance between **product availability and inventory efficiency**. Keeping too little inventory can cause stockouts and lost sales, while keeping too much inventory can lock working capital in products that sell slowly or do not sell at all.

Operational data required to make these decisions is often distributed across product, sales, inventory, procurement, and supplier datasets. Looking at any one dataset independently does not provide enough information to understand the true health of a product.

The goal of this capstone is therefore to build a **Product Performance & Inventory Optimization Intelligence Platform** that integrates product sales, current inventory, inventory movements, procurement, and supplier information to create meaningful product-level analytics.

The platform will focus on four key analytical KPIs:

1. **Sales Velocity**
2. **Days of Stock Cover**
3. **ABC Product Classification**
4. **Inventory Health Classification: Stockout / Healthy / Overstock / Non-Moving**

These KPIs are deliberately connected. Rather than presenting unrelated dashboard metrics, the project will use them together to move from raw data to an actionable product-level recommendation.

---

## 3. Central Business Entity: Product

The entire analytical model is centered on the product.

```text
                         PRODUCT
                            |
          +-----------------+-----------------+
          |                 |                 |
          v                 v                 v
        SALES            INVENTORY        PROCUREMENT
          |                 |                 |
          v                 v                 v
      Units Sold         On Hand          Purchase Qty
      Revenue            Reserved         Suppliers
      Orders             Available        Lead Time
          |                 |                 |
          +-----------------+-----------------+
                            |
                            v
                    PRODUCT INTELLIGENCE
```

### Relevant source datasets

The available synthetic Odoo-style data includes:

```text
products/products.jsonl
products/categories.jsonl
products/supplier_links.jsonl

sales/orders.jsonl
sales/order_lines.jsonl

inventory/quants.jsonl
inventory/moves.jsonl
inventory/move_lines.jsonl

purchase/orders.jsonl
purchase/order_lines.jsonl
```

Not every dataset must contribute to every KPI. Each source should only be introduced where it adds business meaning.

---

# 4. Core KPI 1: Sales Velocity

## Business Question

**How quickly is a product selling during a selected analysis period?**

Total units sold alone can be misleading because it does not account for the period over which the sales occurred. Sales Velocity normalizes sales by time and therefore provides a useful measure of product consumption rate.

## Formula

```text
Sales Velocity = Units Sold During Analysis Period / Number of Days in Analysis Period
```

Example:

```text
Units sold during last 30 days = 300
Analysis period                 = 30 days

Sales Velocity = 300 / 30
               = 10 units/day
```

## Primary data

- `sales/orders.jsonl` provides order dates and order status.
- `sales/order_lines.jsonl` provides product and ordered quantity.

Completed/valid sales states should be defined explicitly so cancelled or invalid orders do not inflate demand.

## Business Meaning

A high sales velocity indicates that inventory is being consumed rapidly. A low sales velocity indicates slower product movement.

Sales Velocity becomes particularly useful when combined with current inventory. A product selling 10 units per day with 500 units available represents a very different situation from a product selling 10 units per day with only 15 units available.

---

# 5. Core KPI 2: Days of Stock Cover

## Business Question

**Based on the current sales rate, approximately how many days can the available inventory support demand?**

This KPI connects sales behavior with current inventory and is one of the central measures of the project.

## Step 1: Available Stock

The inventory quant data provides `quantity` and `reserved_quantity`.

```text
Available Stock = Quantity - Reserved Quantity
```

## Step 2: Days of Stock Cover

```text
Days of Stock Cover = Available Stock / Sales Velocity
```

Example:

```text
On-hand quantity     = 100 units
Reserved quantity    = 20 units
Available stock      = 80 units
Sales velocity       = 10 units/day

Days of Stock Cover = 80 / 10
                    = 8 days
```

## Primary data

- `inventory/quants.jsonl`
- `sales/orders.jsonl`
- `sales/order_lines.jsonl`

## Business Meaning

Very low stock cover may indicate stockout risk. Very high stock cover may indicate excess inventory, especially for products with weak sales.

The interpretation should depend on business-defined thresholds rather than treating every high or low value as automatically problematic.

### Important Edge Case

If Sales Velocity is zero, Days of Stock Cover should not simply be represented as a normal numeric value. A product with inventory but no sales should instead be evaluated for **non-moving inventory**.

---

# 6. Core KPI 3: ABC Product Classification

## Business Question

**Which products are most important to the business based on their contribution to sales revenue?**

ABC analysis prevents every SKU from being treated with equal priority.

## Method

For each product:

```text
Product Revenue = Sum of price_subtotal from valid sales order lines
```

Then:

1. Calculate revenue by product.
2. Sort products from highest to lowest revenue.
3. Calculate each product's percentage contribution to total revenue.
4. Calculate cumulative revenue contribution.
5. Assign an ABC classification based on documented business thresholds.

Conceptually:

```text
A Class -> Products responsible for the highest cumulative revenue contribution
B Class -> Products with medium contribution
C Class -> Remaining long-tail products
```

The exact percentage boundaries should be treated as configurable business rules rather than universal constants.

## Primary data

- `sales/order_lines.jsonl`
- `sales/orders.jsonl`
- `products/products.jsonl`

## Business Meaning

ABC classification establishes **business priority**.

For example, two products may both have low stock cover, but an A-class product that contributes heavily to revenue deserves greater attention than a low-contribution C-class product.

This allows inventory prioritization to consider both operational risk and commercial importance.

---

# 7. Core KPI 4: Inventory Health Classification

## Business Question

**Which products require inventory action, and what type of action is required?**

Inventory Health is the main decision-oriented KPI of the capstone. It consumes insights from the other KPIs rather than operating independently.

The intended classifications are:

```text
STOCKOUT RISK
HEALTHY
OVERSTOCK
NON-MOVING
```

## Example Decision Logic

Final thresholds will be documented as configurable business rules once the dataset has been profiled.

### Stockout Risk

A product may be considered at risk when available stock is insufficient relative to its sales velocity and replenishment requirements.

A stronger version can incorporate supplier lead time:

```text
Days of Stock Cover < Supplier Lead Time
                 |
                 v
       Potential Stockout Risk
```

### Healthy

A product has sufficient stock to satisfy expected short-term consumption without holding excessive inventory.

### Overstock

A product has significantly more stock than its current sales rate justifies.

```text
Low Sales Velocity
       +
High Available Stock
       +
Very High Days of Cover
       |
       v
Potential Overstock
```

### Non-Moving

A product has inventory available but recorded sales velocity is zero during the agreed analysis window.

```text
Available Stock > 0
        +
Sales Velocity = 0
        |
        v
NON-MOVING INVENTORY
```

## Optional Procurement Enhancement

Supplier information can make stockout detection more meaningful. The available supplier-link dataset contains a `delay` field, which can be treated as supplier lead time if that is how the synthetic generator defines the field.

A future replenishment metric could use:

```text
Reorder Point = Average Daily Sales x Lead Time + Safety Stock
```

The capstone should only apply this formula after defining the safety-stock business rule and confirming the semantics of supplier lead time.

---

# 8. How the Four KPIs Work Together

The strength of the project comes from combining the four KPIs rather than displaying them separately.

```text
                       SALES DATA
                           |
                           v
                    SALES VELOCITY
                           |
                           +-------------------+
                           |                   |
                           v                   v
                    CURRENT STOCK        PRODUCT REVENUE
                           |                   |
                           v                   v
                  DAYS OF STOCK COVER     ABC CLASS
                           |                   |
                           +---------+---------+
                                     |
                                     v
                          INVENTORY HEALTH
                                     |
                    +----------------+----------------+
                    |                |                |
                    v                v                v
                STOCKOUT          HEALTHY         OVERSTOCK
                  RISK                                 |
                                                       v
                                                   NON-MOVING
```

A final analytical record could contain fields such as:

```text
product_id
product_name
category
revenue
units_sold
sales_velocity
abc_class
on_hand_quantity
reserved_quantity
available_stock
days_of_stock_cover
supplier_lead_time
inventory_health
recommended_action
```

---

# 9. Product Action Matrix

The final output should help decision-makers understand what action might be needed.

| Product Situation | Interpretation | Possible Business Action |
|---|---|---|
| A-class + high velocity + very low stock cover | Revenue-critical product facing stockout risk | Prioritize replenishment |
| Healthy velocity + adequate stock cover | Balanced inventory | Continue monitoring |
| Low velocity + excessive stock cover | Excess inventory | Review or reduce future purchases |
| Zero sales velocity + stock available | Non-moving inventory | Investigate clearance, discontinuation, or catalog issue |
| C-class + low velocity + high inventory | Capital tied up in a low-priority SKU | Reduce replenishment priority |

The recommended action is analytical decision support. It does not automatically execute inventory decisions.

---

# 10. Supporting Dashboard Metrics

The dashboard can still include simpler descriptive metrics to provide context, but these are **supporting metrics rather than the central analytical KPIs**.

Examples include:

- Total product revenue
- Units sold
- Current inventory
- Available inventory
- Reserved inventory
- Number of active products
- Revenue by category
- Top products by revenue
- Top products by quantity sold
- Product order count
- Average selling price

The dashboard should ultimately lead the user toward the four core analytical KPIs rather than becoming a collection of unrelated charts.

---

# 11. Synthetic Data Strategy

Because the supplied dataset is synthetic, the dataset can be enriched to ensure that important business conditions are represented. Synthetic records should be generated to represent plausible business scenarios rather than modifying individual values simply to force desired dashboard outcomes.

The data should contain examples of at least the following scenarios:

### Scenario A: High-priority stockout risk

```text
ABC Class       = A
Sales Velocity  = High
Available Stock = Low
Stock Cover     = Low
Lead Time       = Greater than stock cover
```

Expected insight: **Critical replenishment priority**.

### Scenario B: Overstock

```text
Sales Velocity  = Low
Available Stock = High
Stock Cover     = Very High
```

Expected insight: **Potential excess inventory**.

### Scenario C: Non-moving product

```text
Sales Velocity  = Zero
Available Stock > 0
```

Expected insight: **Inventory is being held without recent sales movement**.

### Scenario D: Healthy product

```text
Sales Velocity = Normal
Stock Cover    = Within the agreed healthy range
```

Expected insight: **No immediate inventory intervention required**.

### Scenario E: C-class excess inventory

```text
ABC Class       = C
Sales Velocity  = Low
Available Stock = High
```

Expected insight: **Low-value inventory may be consuming unnecessary working capital and storage capacity**.

---

# 12. Scope Boundaries

To keep the capstone focused, the following items are intentionally outside the core business scope:

- Full customer lifecycle analytics
- Customer segmentation
- Complete order-to-cash analytics
- Payment collection analytics
- Salesperson performance analytics
- End-to-end supplier performance analytics
- Machine-learning demand forecasting
- Demand trend as a standalone KPI

Procurement and supplier information is used only where it helps explain **product inventory and replenishment risk**.

---

# 13. Expected Business Outcome

The platform should transform product-level operational data into three layers of understanding:

```text
WHAT HAPPENED?
Revenue, Units Sold, Current Stock
            |
            v
WHAT DOES IT MEAN?
Sales Velocity, ABC Class, Days of Stock Cover
            |
            v
WHAT REQUIRES ATTENTION?
Stockout Risk, Healthy Stock, Overstock, Non-Moving Stock
            |
            v
WHAT SHOULD THE BUSINESS CONSIDER DOING?
Replenish, Monitor, Reduce Purchasing, Investigate/Clear Stock
```

The key value of the capstone is therefore not simply a product dashboard. It is the creation of a **product-level decision-support data product** that helps the business balance product availability with inventory efficiency.

---

# 14. Final Capstone Statement

> **Design and implement a Product Performance & Inventory Optimization Intelligence Platform using synthetic Odoo e-commerce data. The platform will integrate product, sales, inventory, procurement, and supplier information to measure Sales Velocity, Days of Stock Cover, ABC Product Classification, and Inventory Health. The resulting analytics will enable identification and prioritization of revenue-critical stockout risks, healthy inventory, excess stock, and non-moving products, providing actionable product-level inventory insights for business decision-making.**
