-- Apply to the approved NorthwindLab Azure SQL database after the sample data is loaded.
-- Grant its managed export identity SELECT on this view only.
CREATE OR ALTER VIEW dbo.PortfolioOrderLines AS
SELECT o.OrderID AS order_id, CONVERT(date, o.OrderDate) AS order_date,
       d.ProductID AS product_id, CONVERT(int, d.Quantity) AS quantity,
       CONVERT(decimal(19,4), d.UnitPrice * 100) AS unit_price_minor,
       CONVERT(int, ROUND(CONVERT(decimal(12,8), d.Discount) * 10000, 0)) AS discount_bps
FROM dbo.Orders AS o
JOIN dbo.[Order Details] AS d ON d.OrderID = o.OrderID;
