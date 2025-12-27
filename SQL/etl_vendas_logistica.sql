/*
  PROJETO: Análise de Performance E-commerce Olist
  ARQUIVO: etl_vendas_logistica.sql
  AUTOR: Bruno Maciel - Economista & Analista de Dados
  
  DESCRIÇÃO:
  Esta query realiza a modelagem dos dados (ETL) para importação no Power BI.
  O objetivo é consolidar dados de diversas tabelas transacionais em uma visão analítica única por Pedido.
  
  DESTAQUES TÉCNICOS:
  1. Uso de CTEs (Common Table Expressions): Utilizado para pré-agregar tabelas filhas (pagamentos e itens).
     Isso evita o problema de "Fan-out" (duplicação de linhas) que ocorreria ao fazer múltiplos LEFT JOINS
     diretos em tabelas 1:N, garantindo a precisão das somas monetárias.
  
  2. Categorização Regional: Regra de negócio aplicada via CASE WHEN para agrupar estados em regiões.
  
  3. Cálculo de KPIs Logísticos: Lead time de entrega e status de atraso calculados na fonte.
*/

-- 1. CTE para agregar valores de pagamentos (evita duplicação ao cruzar com itens)
WITH PagamentosAgregados AS (
    SELECT 
        order_id,
        SUM(payment_value) AS valor_total_pagamento,
        GROUP_CONCAT(DISTINCT payment_type SEPARATOR ', ') AS metodos_pagamento
    FROM pagamentos
    GROUP BY order_id
),

-- 2. CTE para agregar valores dos itens (produtos e frete)
ItensAgregados AS (
    SELECT 
        order_id,
        SUM(price) AS valor_total_produtos,
        SUM(freight_value) AS valor_total_frete
    FROM itens_pedidos
    GROUP BY order_id
),

-- 3. CTE para calcular a média de avaliações (evita múltiplas notas para o mesmo pedido)
ReviewsUnicos AS (
    SELECT 
        order_id, 
        AVG(review_score) as review_score
    FROM reviews
    GROUP BY order_id 
)

-- 4. Consulta Principal: Consolidação das CTEs com a tabela de Pedidos e Clientes
SELECT 
    o.order_id,
    c.customer_unique_id,
    o.order_status,
    c.customer_city,
    c.customer_state,
    
    -- Regra de Negócio: Agrupamento Regional
    CASE 
        WHEN c.customer_state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Sudeste'
        WHEN c.customer_state IN ('PR', 'SC', 'RS') THEN 'Sul'
        WHEN c.customer_state IN ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Nordeste'
        WHEN c.customer_state IN ('MT', 'MS', 'GO', 'DF') THEN 'Centro-Oeste'
        WHEN c.customer_state IN ('AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC') THEN 'Norte'
        ELSE 'Outros'
    END AS regiao_cliente,
    
    -- Datas e Prazos
    o.order_purchase_timestamp AS dt_compra,
    o.order_delivered_customer_date AS dt_entrega,
    o.order_estimated_delivery_date AS dt_prometida,
    
    -- KPIs de Tempo (DATEDIFF)
    DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS dias_para_entrega,
    DATEDIFF(o.order_estimated_delivery_date, o.order_purchase_timestamp) AS dias_ate_promessa,
    
    -- Status Logístico
    CASE 
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 'Atrasado'
        ELSE 'No Prazo'
    END AS status_entrega,
    
    -- Métricas Financeiras (Tratamento de Nulos com COALESCE)
    COALESCE(p.valor_total_pagamento, 0) AS valor_total_pagamento,
    COALESCE(i.valor_total_produtos, 0) AS valor_total_produtos,
    COALESCE(i.valor_total_frete, 0) AS valor_total_frete,
    
    p.metodos_pagamento,
    r.review_score as nota_review

FROM pedidos o
JOIN clientes c ON o.customer_id = c.customer_id
LEFT JOIN PagamentosAgregados p ON o.order_id = p.order_id
LEFT JOIN ItensAgregados i ON o.order_id = i.order_id
LEFT JOIN ReviewsUnicos r ON o.order_id = r.order_id

WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL;
