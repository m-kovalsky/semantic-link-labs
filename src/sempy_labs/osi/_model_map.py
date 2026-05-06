model_map = {
    "model": {
        "name": "",
        "description": "",
        "tables": [
            {
                "tableName": "Segment",
                "description": "This is a segment table.",
                "sourceName": "database.schema.table",
                "sourceitemId": "", # Map to a fabric item
                "sourceworkspaceId": "", # Map to a fabric workspace
                "columns": [
                    {
                        "name": "Summary Segment",
                        "sourceColumn": "Segment",
                        "dataType": "String",
                        "format_original": "",
                        "format_pbi": "General",
                        "description": "This is a description.",
                        "expression": "",
                        "synonyms": [],
                        "fullDAXObjectName": "'Segment'[Summary Segment]",
                        "isCalculated": False,
                    }
                ],
                "measures": [
                    {
                        "name": "",
                        "expression_original": "",
                        "expression_dax": "",
                        "format_original": "",
                        "format_pbi": "",
                        "description": "",
                        "synonyms": [],
                    }
                ],
            }
        ],
        "relationships": [
            {
                "fromTable": "",
                "fromColumn": "",
                "toTable": "",
                "toColumn": "",
                "fromCardinality": "Many",
                "toCardinality": "One",
            }
        ],
    }
}


source_map = {
    "sourceName": {
        ""
    }
}