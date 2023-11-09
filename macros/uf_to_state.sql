{% macro uf_to_state(uf) %}

  CASE {{ uf }}
    WHEN 'RS' THEN 'Rio Grande do Sul'
    WHEN 'SC' THEN 'Santa Catarina'
    WHEN 'PR' THEN 'Paraná'
    WHEN 'MS' THEN 'Mato Grosso do Sul'
    WHEN 'GO' THEN 'Goiás'
    WHEN 'MT' THEN 'Mato Grosso'
    WHEN 'DF' THEN 'Distrito Federal'
    WHEN 'SP' THEN 'São Paulo'
    WHEN 'RJ' THEN 'Rio de Janeiro'
    WHEN 'MG' THEN 'Minas Gerais'
    WHEN 'ES' THEN 'Espírito Santo'
    WHEN 'BA' THEN 'Bahia'
    WHEN 'SE' THEN 'Sergipe'
    WHEN 'AL' THEN 'Alagoas'
    WHEN 'PE' THEN 'Pernambuco'
    WHEN 'PB' THEN 'Paraíba'
    WHEN 'RN' THEN 'Rio Grande do Norte'
    WHEN 'CE' THEN 'Ceará'
    WHEN 'PI' THEN 'Piauí'
    WHEN 'MA' THEN 'Maranhão'
    WHEN 'TO' THEN 'Tocantins'
    WHEN 'PA' THEN 'Pará'
    WHEN 'AM' THEN 'Amazonas'
    WHEN 'RR' THEN 'Roraima'
    WHEN 'RO' THEN 'Rondônia'
    WHEN 'AC' THEN 'Acre'
    WHEN 'AP' THEN 'Amapá'
  END
{% endmacro %}