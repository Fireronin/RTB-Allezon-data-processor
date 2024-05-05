-- ------------------------------
-- OPTION
-- ------------------------------

OPTION IMPORT;

DEFINE NAMESPACE IF NOT EXISTS test;
DEFINE DATABASE IF NOT EXISTS test;

-- ------------------------------
-- TABLE: tags
-- ------------------------------

DEFINE TABLE view_tags SCHEMAFULL;
DEFINE TABLE buy_tags SCHEMAFULL;

DEFINE FIELD tags ON TABLE view_tags TYPE array<object>;
DEFINE FIELD tags ON TABLE buy_tags TYPE array<object>;
