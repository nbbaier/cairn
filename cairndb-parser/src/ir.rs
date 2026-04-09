use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable {
        table: String,
    },
    Insert {
        table: String,
        data: Map<String, Value>,
    },
    Select {
        table: String,
        filter: Option<Filter>,
        temporal: Option<TemporalClause>,
    },
    Update {
        table: String,
        data: Map<String, Value>,
        filter: Filter,
    },
    Delete {
        table: String,
        filter: Filter,
    },
    Erase {
        table: String,
        filter: Filter,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    ById(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TemporalClause {
    AsOf(String),
    Between(String, String),
    All,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn create_table_construction() {
        let stmt = Statement::CreateTable {
            table: "events".to_string(),
        };
        assert_eq!(
            stmt,
            Statement::CreateTable {
                table: "events".to_string()
            }
        );
    }

    #[test]
    fn insert_construction() {
        let mut data = Map::new();
        data.insert("name".to_string(), json!("Alice"));
        let stmt = Statement::Insert {
            table: "users".to_string(),
            data: data.clone(),
        };
        if let Statement::Insert { table, data: d } = stmt {
            assert_eq!(table, "users");
            assert_eq!(d, data);
        } else {
            panic!("expected Insert");
        }
    }

    #[test]
    fn select_construction() {
        let stmt = Statement::Select {
            table: "events".to_string(),
            filter: Some(Filter::ById("abc".to_string())),
            temporal: Some(TemporalClause::All),
        };
        if let Statement::Select {
            table,
            filter,
            temporal,
        } = stmt
        {
            assert_eq!(table, "events");
            assert_eq!(filter, Some(Filter::ById("abc".to_string())));
            assert_eq!(temporal, Some(TemporalClause::All));
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn temporal_clause_variants() {
        let as_of = TemporalClause::AsOf("2025-01-01T00:00:00.000Z".to_string());
        let between = TemporalClause::Between(
            "2025-01-01T00:00:00.000Z".to_string(),
            "2025-12-31T00:00:00.000Z".to_string(),
        );
        let all = TemporalClause::All;

        assert_ne!(as_of, between);
        assert_ne!(between, all);
        assert_ne!(as_of, all);
    }

    #[test]
    fn clone_and_debug() {
        let stmt = Statement::CreateTable {
            table: "t".to_string(),
        };
        let cloned = stmt.clone();
        assert_eq!(stmt, cloned);
        let debug = format!("{:?}", stmt);
        assert!(debug.contains("CreateTable"));
    }
}
