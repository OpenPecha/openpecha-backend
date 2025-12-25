"""Integration test for relations endpoint using real Neo4j test instance.

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
"""
import json
import os
import pytest
from dotenv import load_dotenv
from models import (
    PersonModelInput,
    ExpressionModelInput,
    ManifestationModelInput,
    AlignedTextRequestModel
)
from main import create_app
from neo4j_database import Neo4JDatabase

load_dotenv()

@pytest.fixture(scope="session")
def neo4j_connection():
    """Get Neo4j connection details from environment variables"""
    test_uri = os.environ.get("NEO4J_TEST_URI")
    test_password = os.environ.get("NEO4J_TEST_PASSWORD")

    if not test_uri or not test_password:
        pytest.skip(
            "Neo4j test credentials not provided. Set NEO4J_TEST_URI and NEO4J_TEST_PASSWORD environment variables."
        )

    yield {"uri": test_uri, "auth": ("neo4j", test_password)}


@pytest.fixture
def test_database(neo4j_connection):
    """Create a Neo4JDatabase instance connected to the test Neo4j instance"""
    # Set environment variables so API endpoints can connect to test database
    os.environ["NEO4J_URI"] = neo4j_connection["uri"]
    os.environ["NEO4J_PASSWORD"] = neo4j_connection["auth"][1]

    # Create Neo4j database with test connection
    db = Neo4JDatabase(neo4j_uri=neo4j_connection["uri"], neo4j_auth=neo4j_connection["auth"])

    # Setup test schema and basic data
    with db.get_session() as session:
        # Clean up any existing data first
        session.run("MATCH (n) DETACH DELETE n")

        # Create test languages
        session.run("MERGE (l:Language {code: 'bo', name: 'Tibetan'})")
        session.run("MERGE (l:Language {code: 'tib', name: 'Spoken Tibetan'})")
        session.run("MERGE (l:Language {code: 'en', name: 'English'})")
        session.run("MERGE (l:Language {code: 'sa', name: 'Sanskrit'})")
        session.run("MERGE (l:Language {code: 'zh', name: 'Chinese'})")

        # Create test text types (TextType enum values)
        session.run("MERGE (t:TextType {name: 'root'})")
        session.run("MERGE (t:TextType {name: 'commentary'})")
        session.run("MERGE (t:TextType {name: 'translation'})")

        # Create test role types (only allowed values per constraints)
        session.run("MERGE (r:RoleType {name: 'translator'})")
        session.run("MERGE (r:RoleType {name: 'author'})")
        session.run("MERGE (r:RoleType {name: 'reviser'})")

        # Create test license type
        session.run("MERGE (l:License {name: 'CC0'})")

    yield db

    # Cleanup after test
    with db.get_session() as session:
        session.run("MATCH (n) DETACH DELETE n")


@pytest.fixture
def client():
    """Create Flask test client"""
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


@pytest.fixture
def test_person_data():
    """Sample person data for testing"""
    return {
        "name": {"en": "Test Author", "bo": "སློབ་དཔོན།"},
        "alt_names": [{"en": "Alternative Name", "bo": "མིང་གཞན།"}],
        "bdrc": "P123456",
        "wiki": "Q123456",
    }

@pytest.fixture
def test_expression_data():
    """Sample expression data for testing"""
    return {
        "type": "root",
        "title": {"en": "Test Expression", "bo": "བརྟག་དཔྱད་ཚིག་སྒྲུབ།"},
        "alt_titles": [{"en": "Alternative Title", "bo": "མཚན་བྱང་གཞན།"}],
        "language": "en",
        "contributions": [],  # Will be populated with actual person IDs
        "date": "2024-01-01",
        "bdrc": "W123456",
        "wiki": "Q789012",
    }

class TestGetSegmentRelationV2:

    def _create_category(self, test_database) -> str:
        """Create a test category"""
        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        return category_id
    
    def _create_person(self, test_database, test_person_data) -> str:
        """Create a test person"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
        return person_id

    def _create_expression_and_manifestation_for_text_A(
        self,
        client,
        test_database,
        test_expression_data,
        test_person_data
    ) -> tuple[str, str]:
        """Create a test expression"""
        person_id = self._create_person(test_database, test_person_data)
        category_id = self._create_category(test_database)

        expression_data = test_expression_data
        expression_data["title"] = "Bo root expression"
        expression_data["language"] = "bo"
        expression_data["category_id"] = category_id
        expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(expression_data)
        
        expression_response = client.post("/v2/texts", data=json.dumps(expression), content_type="application/json")
        
        assert expression_response.status_code == 201
        data = json.loads(expression_response.data)
        expression_id = data["id"]

        manifestation_data = {
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 8
                    }
                },
                {
                    "span": {
                        "start": 8,
                        "end": 18
                    }
                },
                {
                    "span": {
                        "start": 18,
                        "end": 26
                    }
                },
                {
                    "span": {
                        "start": 26,
                        "end": 34
                    }
                }
            ],
            "content": "This is the text content to be stored"
        }
        manifestation = ManifestationModelInput.model_validate(manifestation_data)
        manifestation_response = client.post("/v2/manifestations", data=json.dumps(manifestation), content_type="application/json")
        
        assert manifestation_response.status_code == 201
        data = json.loads(manifestation_response.data)
        manifestation_id = data["id"]
        
        return expression_id, manifestation_id

    def _create_text_B_translation_target_text_A(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test translation"""

        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        translation_request = {
            "language": "bo",
            "content": "This is the translated text content",
            "title": "B. En translation with target A",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 7
                    }
                },
                {
                    "span": {
                        "start": 7,
                        "end": 17
                    }
                },
                {
                    "span": {
                        "start": 17,
                        "end": 41
                    }
                },
                {
                    "span": {
                        "start": 41,
                        "end": 51
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 18
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 18,
                        "end": 26
                    },
                    "index": 1
                },
                {
                    "span": {
                        "start": 26,
                        "end": 34
                    },
                    "index": 2
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 17
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 17,
                        "end": 41
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                },
                {
                    "span": {
                        "start": 41,
                        "end": 51
                    },
                    "index": 2,
                    "alignment_index": [
                        2
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }

        translation = AlignedTextRequestModel.model_validate(translation_request)

        translation_response = client.post(
            f"/v2/instances/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )

        assert translation_response.status_code == 201
        data = translation_response.get_json()
        translation_id = data["id"]
        return translation_id

    def _create_text_C_commentary_target_text_A(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test commentary"""

        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        commentary_request = {
            "language": "bo",
            "content": "This is the translated text content",
            "title": "C. Bo commentary with target A",
            "category_id": category_id,
            "source": "Source of the commentary",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 16
                    }
                },
                {
                    "span": {
                        "start": 16,
                        "end": 36
                    }
                },
                {
                    "span": {
                        "start": 36,
                        "end": 54
                    }
                },
                {
                    "span": {
                        "start": 54,
                        "end": 71
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 8
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 8,
                        "end": 26
                    },
                    "index": 1
                },
                {
                    "span": {
                        "start": 26,
                        "end": 34
                    },
                    "index": 2
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 16
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 16,
                        "end": 54
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                },
                {
                    "span": {
                        "start": 54,
                        "end": 71
                    },
                    "index": 2,
                    "alignment_index": [
                        2
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }
        commentary = AlignedTextRequestModel.model_validate(commentary_request)
        commentary_response = client.post(
            f"/v2/instances/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )

        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        commentary_id = data["id"]
        return commentary_id
    
    def _create_text_D_commentary_target_text_B(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test commentary"""
        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        commentary_request = {
            "language": "en",
            "content": "This is the translated text content",
            "title": "D. En commentary with target B",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 15
                    }
                },
                {
                    "span": {
                        "start": 15,
                        "end": 39
                    }
                },
                {
                    "span": {
                        "start": 39,
                        "end": 70
                    }
                },
                {
                    "span": {
                        "start": 70,
                        "end": 91
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 17
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 17,
                        "end": 51
                    },
                    "index": 1
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 10
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 10,
                        "end": 60
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                },
                {
                    "span": {
                        "start": 60,
                        "end": 80
                    },
                    "index": 2,
                    "alignment_index": [
                        2
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }

        commentary = AlignedTextRequestModel.model_validate(commentary_request)
        commentary_response = client.post(
            f"/v2/instances/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )

        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        commentary_id = data["id"]
        return commentary_id

    def _create_text_E_translation_target_text_A(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test translation"""
        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        translation_request = {
            "language": "fr",
            "content": "This is the translated text content",
            "title": "E. Fr translation with target A",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 7
                    }
                },
                {
                    "span": {
                        "start": 7,
                        "end": 18
                    }
                },
                {
                    "span": {
                        "start": 18,
                        "end": 44
                    }
                },
                {
                    "span": {
                        "start": 44,
                        "end": 56
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 8
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 8,
                        "end": 18
                    },
                    "index": 1
                },
                {
                    "span": {
                        "start": 18,
                        "end": 34
                    },
                    "index": 2
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 7
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 7,
                        "end": 18
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                },
                {
                    "span": {
                        "start": 18,
                        "end": 56
                    },
                    "index": 2,
                    "alignment_index": [
                        2
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
            }

        translation = AlignedTextRequestModel.model_validate(translation_request)
        translation_response = client.post(
            f"/v2/instances/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        data = translation_response.get_json()
        translation_id = data["id"]
        return translation_id

    def _create_text_F_commentary_target_text_E(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test commentary"""
        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        commentary_request = {
            "language": "fr",
            "content": "This is the translated text content",
            "title": "F. Fr commentary with target E",
            "category_id": category_id,
            "source": "Source of the commentary",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 17
                    }
                },
                {
                    "span": {
                        "start": 17,
                        "end": 35
                    }
                },
                {
                    "span": {
                        "start": 35,
                        "end": 70
                    }
                },
                {
                    "span": {
                        "start": 70,
                        "end": 91
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 10
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 18,
                        "end": 40
                    },
                    "index": 1
                },
                {
                    "span": {
                        "start": 40,
                        "end": 56
                    },
                    "index": 2
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 40
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 50,
                        "end": 70
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                },
                {
                    "span": {
                        "start": 70,
                        "end": 80
                    },
                    "index": 2,
                    "alignment_index": [
                        2
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }
    
    def _create_text_G_translation_target_text_C(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test translation"""
        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        translation_request = {
            "language": "lzh",
            "content": "This is the translated text content",
            "title": "J. Lzh translation with target C",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 50
                    }
                },
                {
                    "span": {
                        "start": 50,
                        "end": 55
                    }
                },
                {
                    "span": {
                        "start": 55,
                        "end": 100
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 16
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 16,
                        "end": 36
                    },
                    "index": 1
                },
                {
                    "span": {
                        "start": 36,
                        "end": 54
                    },
                    "index": 2
                },
                {
                    "span": {
                        "start": 54,
                        "end": 71
                    },
                    "index": 3
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 30
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 30,
                        "end": 50
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                },
                {
                    "span": {
                        "start": 50,
                        "end": 55
                    },
                    "index": 2,
                    "alignment_index": [
                        2
                    ]
                },
                {
                    "span": {
                        "start": 55,
                        "end": 100
                    },
                    "index": 3,
                    "alignment_index": [
                        3
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }
        translation = AlignedTextRequestModel.model_validate(translation_request)
        translation_response = client.post(
            f"/v2/instances/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        data = translation_response.get_json()
        translation_id = data["id"]
        return translation_id

    def _create_text_H_commentary_target_text_C(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test commentary"""
        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        commentary_request = {
            "language": "bo",
            "content": "This is the translated text content",
            "title": "H. Bo commentary with target C",
            "category_id": category_id,
            "source": "Source of the commentary",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 10
                    }
                },
                {
                    "span": {
                        "start": 10,
                        "end": 20
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 15
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 15,
                        "end": 20
                    },
                    "index": 1
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 54
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 54,
                        "end": 71
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
            }
        commentary = AlignedTextRequestModel.model_validate(commentary_request)
        commentary_response = client.post(
            f"/v2/instances/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )
        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        commentary_id = data["id"]
        return commentary_id

    def _create_text_I_commentary_target_text_G(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test commentary"""
        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        commentary_request = {
            "language": "lzh",
            "content": "This is the commentary text content",
            "title": "I. Lzh commentary with target G",
            "category_id": category_id,
            "source": "Source of the commentary",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 5
                    }
                },
                {
                    "span": {
                        "start": 5,
                        "end": 15
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 40
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 40,
                        "end": 60
                    },
                    "index": 1
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 7
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 7,
                        "end": 15
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }
        commentary = AlignedTextRequestModel.model_validate(commentary_request)
        commentary_response = client.post(
            f"/v2/instances/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )
        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        commentary_id = data["id"]
        return commentary_id
    
    def _create_text_J_translation_target_text_H(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test translation"""
        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        translation_request = {
            "language": "zh",
            "content": "This is the translated text content",
            "title": "J. Zh translation with target H",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 14
                    }
                },
                {
                    "span": {
                        "start": 14,
                        "end": 30
                    }
                },
                {
                    "span": {
                        "start": 30,
                        "end": 40
                    }
                },
                {
                    "span": {
                        "start": 40,
                        "end": 47
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 5
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 5,
                        "end": 10
                    },
                    "index": 1
                },
                {
                    "span": {
                        "start": 10,
                        "end": 20
                    },
                    "index": 2
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 30
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 30,
                        "end": 35
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                },
                {
                    "span": {
                        "start": 35,
                        "end": 47
                    },
                    "index": 2,
                    "alignment_index": [
                        2
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }
        translation = AlignedTextRequestModel.model_validate(translation_request)
        translation_response = client.post(
            f"/v2/instances/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        data = translation_response.get_json()
        translation_id = data["id"]
        return translation_id

    def _create_text_K_translation_target_text_B(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test translation"""
        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        translation_request = {
            "language": "ja",
            "content": "This is the translated text content",
            "title": "K. ja translation with target B",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    }
                },
                {
                    "span": {
                        "start": 20,
                        "end": 40
                    }
                },
                {
                    "span": {
                        "start": 40,
                        "end": 60
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 7
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 7,
                        "end": 17
                    },
                    "index": 1
                },
                {
                    "span": {
                        "start": 17,
                        "end": 41
                    },
                    "index": 2
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 20,
                        "end": 40
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                },
                {
                    "span": {
                        "start": 40,
                        "end": 60
                    },
                    "index": 2,
                    "alignment_index": [
                        2
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }
        translation = AlignedTextRequestModel.model_validate(translation_request)
        translation_response = client.post(
            f"/v2/instances/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        data = translation_response.get_json()
        translation_id = data["id"]
        return translation_id

    def _create_expression_and_manifestation_for_text_L(
        self,
        client,
        test_database,
        test_expression_data,
        test_person_data
    ) -> tuple[str, str]:
        """Create a test expression"""
        person_id = self._create_person(test_database, test_person_data)
        category_id = self._create_category(test_database)

        expression_data = test_expression_data
        expression_data["title"] = "Sa standalone translationexpression"
        expression_data["language"] = "sa"
        expression_data["target"] = "N/A"
        expression_data["type"] = "translation"
        expression_data["category_id"] = category_id
        expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(expression_data)
        
        expression_response = client.post("/v2/texts", data=json.dumps(expression), content_type="application/json")
        
        assert expression_response.status_code == 201
        data = json.loads(expression_response.data)
        expression_id = data["id"]

        manifestation_data = {
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    }
                },
                {
                    "span": {
                        "start": 20,
                        "end": 40
                    }
                },
                {
                    "span": {
                        "start": 40,
                        "end": 60
                    }
                },
                {
                    "span": {
                        "start": 60,
                        "end": 80
                    }
                },
                {
                    "span": {
                        "start": 80,
                        "end": 100
                    }
                }
            ],
            "content": "This is the text content to be stored"
        }
        manifestation = ManifestationModelInput.model_validate(manifestation_data)
        manifestation_response = client.post("/v2/manifestations", data=json.dumps(manifestation), content_type="application/json")
        
        assert manifestation_response.status_code == 201
        data = json.loads(manifestation_response.data)
        manifestation_id = data["id"]
        
        return expression_id, manifestation_id

    def _create_text_M_translation_target_text_L(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test translation"""
        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        translation_request = {
            "language": "bo",
            "content": "This is the translated text content",
            "title": "M. Bo translation with target L",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 30
                    }
                },
                {
                    "span": {
                        "start": 30,
                        "end": 40
                    }
                },
                {
                    "span": {
                        "start": 40,
                        "end": 70
                    }
                },
                {
                    "span": {
                        "start": 70,
                        "end": 80
                    }
                },
                {
                    "span": {
                        "start": 80,
                        "end": 100
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 40
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 40,
                        "end": 60
                    },
                    "index": 1
                },
                {
                    "span": {
                        "start": 60,
                        "end": 100
                    },
                    "index": 2
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 30
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 30,
                        "end": 70
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                },
                {
                    "span": {
                        "start": 70,
                        "end": 100
                    },
                    "index": 2,
                    "alignment_index": [
                        2
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }
        translation = AlignedTextRequestModel.model_validate(translation_request)
        translation_response = client.post(
            f"/v2/instances/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        data = translation_response.get_json()
        translation_id = data["id"]
        return translation_id
    
    def _create_text_N_commentary_target_text_L(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test commentary"""
        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        commentary_request = {
            "language": "Bo",
            "content": "This is the commentary text content",
            "title": "N. Bo commentary with target L",
            "category_id": category_id,
            "source": "Source of the commentary",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 30
                    }
                },
                {
                    "span": {
                        "start": 30,
                        "end": 50
                    }
                },
                {
                    "span": {
                        "start": 50,
                        "end": 100
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "index": 0
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 100
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }
        commentary = AlignedTextRequestModel.model_validate(commentary_request)
        commentary_response = client.post(
            f"/v2/instances/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )
        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        commentary_id = data["id"]
        return commentary_id

    def _create_expression_and_manifestation_for_text_O(
        self,
        client,
        test_database,
        test_expression_data,
        test_person_data
    ) -> tuple[str, str]:
        """Create a test expression"""
        person_id = self._create_person(test_database, test_person_data)
        category_id = self._create_category(test_database)

        expression_data = test_expression_data
        expression_data["title"] = "Bo standalone commentary expression"
        expression_data["language"] = "bo"
        expression_data["target"] = "N/A"
        expression_data["type"] = "commentary"
        expression_data["category_id"] = category_id
        expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(expression_data)
        
        expression_response = client.post("/v2/texts", data=json.dumps(expression), content_type="application/json")
        
        assert expression_response.status_code == 201
        data = json.loads(expression_response.data)
        expression_id = data["id"]

        manifestation_data = {
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 30
                    }
                },
                {
                    "span": {
                        "start": 30,
                        "end": 80
                    }
                },
                {
                    "span": {
                        "start": 80,
                        "end": 120
                    }
                },
                {
                    "span": {
                        "start": 120,
                        "end": 350
                    }
                }
            ],
            "content": "This is the text content to be stored"
        }
        manifestation = ManifestationModelInput.model_validate(manifestation_data)
        manifestation_response = client.post("/v2/manifestations", data=json.dumps(manifestation), content_type="application/json")
        
        assert manifestation_response.status_code == 201
        data = json.loads(manifestation_response.data)
        manifestation_id = data["id"]
        
        return expression_id, manifestation_id

    def _create_text_P_commentary_target_text_O(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test commentary"""
        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        commentary_request = {
            "language": "En",
            "content": "This is the commentary text content",
            "title": "P. En commentary with target O",
            "category_id": category_id,
            "source": "Source of the commentary",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 25
                    }
                },
                {
                    "span": {
                        "start": 25,
                        "end": 60
                    }
                },
                {
                    "span": {
                        "start": 60,
                        "end": 100
                    }
                },
                {
                    "span": {
                        "start": 100,
                        "end": 285
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 30
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 30,
                        "end": 80
                    },
                    "index": 1
                },
                {
                    "span": {
                        "start": 80,
                        "end": 120
                    },
                    "index": 2
                },
                {
                    "span": {
                        "start": 120,
                        "end": 350
                    },
                    "index": 3
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 25
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 25,
                        "end": 60
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                },
                {
                    "span": {
                        "start": 60,
                        "end": 100
                    },
                    "index": 2,
                    "alignment_index": [
                        2
                    ]
                },
                {
                    "span": {
                        "start": 100,
                        "end": 285
                    },
                    "index": 3,
                    "alignment_index": [
                        3
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }
        commentary = AlignedTextRequestModel.model_validate(commentary_request)
        commentary_response = client.post(
            f"/v2/instances/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )
        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        commentary_id = data["id"]
        return commentary_id

    def _create_text_Q_translation_target_text_O(
        self,
        client,
        test_database,
        test_person_data,
        target_manifestation_id
    ) -> str:
        """Create a test translation"""
        category_id = self._create_category(test_database)
        person_id = self._create_person(test_database, test_person_data)

        translation_request = {
            "language": "bo",
            "content": "This is the translated text content",
            "title": "Q. Bo translation with target O",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 40
                    }
                },
                {
                    "span": {
                        "start": 40,
                        "end": 140
                    }
                },
                {
                    "span": {
                        "start": 140,
                        "end": 250
                    }
                },
                {
                    "span": {
                        "start": 250,
                        "end": 400
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 30
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 120,
                        "end": 350
                    },
                    "index": 1
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 40
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 250,
                        "end": 400
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }
        translation = AlignedTextRequestModel.model_validate(translation_request)
        translation_response = client.post(
            f"/v2/instances/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        data = translation_response.get_json()
        translation_id = data["id"]
        return translation_id


    