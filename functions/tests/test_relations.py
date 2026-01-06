"""Integration test for relations endpoint using real Neo4j test instance.

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
"""
import copy
import json

import pytest
from models import ExpressionInput, PersonInput
from request_models import EditionRequestModel


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

@pytest.fixture(autouse=True)
def all_texts(client, test_database, test_expression_data, test_person_data):
    """
    Autouse fixture: creates the full A..N dataset before every test.

    Note: test_database wipes Neo4j per-test, so this fixture is intentionally
    function-scoped (default) and will recreate the dataset for each test.
    """
    creator = TestGetSegmentRelationV2()

    category_id = creator._create_category(test_database)
    person_id = creator._create_person(test_database, test_person_data)

    text_a_id, edition_a_id = creator._create_expression_and_manifestation_for_text_A(
        client, copy.deepcopy(test_expression_data), category_id, person_id
    )
    text_b_id, edition_b_id = creator._create_text_B_translation_target_text_A(
        client, category_id, person_id, edition_a_id
    )
    text_c_id, edition_c_id = creator._create_text_C_commentary_target_text_A(
        client, category_id, person_id, edition_a_id
    )
    text_d_id, edition_d_id = creator._create_text_D_commentary_target_text_B(
        client, category_id, person_id, edition_b_id
    )
    text_e_id, edition_e_id = creator._create_text_E_translation_target_text_A(
        client, category_id, person_id, edition_a_id
    )
    text_f_id, edition_f_id = creator._create_text_F_commentary_target_text_E(
        client, category_id, person_id, edition_e_id
    )
    text_g_id, edition_g_id = creator._create_text_G_translation_target_text_C(
        client, category_id, person_id, edition_c_id
    )
    text_h_id, edition_h_id = creator._create_text_H_commentary_target_text_C(
        client, category_id, person_id, edition_c_id
    )
    text_i_id, edition_i_id = creator._create_text_I_commentary_target_text_G(
        client, category_id, person_id, edition_g_id
    )
    text_j_id, edition_j_id = creator._create_text_J_translation_target_text_H(
        client, category_id, person_id, edition_h_id
    )
    text_k_id, edition_k_id = creator._create_text_K_translation_target_text_B(
        client, category_id, person_id, edition_b_id
    )
    text_l_id, edition_l_id = creator._create_expression_and_manifestation_for_text_L(
        client, copy.deepcopy(test_expression_data), category_id, person_id
    )
    text_m_id, edition_m_id = creator._create_text_M_translation_target_text_L(
        client, category_id, person_id, edition_l_id
    )
    text_n_id, edition_n_id = creator._create_text_N_commentary_target_text_L(
        client, category_id, person_id, edition_l_id
    )

    return {
        "category_id": category_id,
        "person_id": person_id,
        "A": {"text_id": text_a_id, "edition_id": edition_a_id},
        "B": {"text_id": text_b_id, "edition_id": edition_b_id},
        "C": {"text_id": text_c_id, "edition_id": edition_c_id},
        "D": {"text_id": text_d_id, "edition_id": edition_d_id},
        "E": {"text_id": text_e_id, "edition_id": edition_e_id},
        "F": {"text_id": text_f_id, "edition_id": edition_f_id},
        "G": {"text_id": text_g_id, "edition_id": edition_g_id},
        "H": {"text_id": text_h_id, "edition_id": edition_h_id},
        "I": {"text_id": text_i_id, "edition_id": edition_i_id},
        "J": {"text_id": text_j_id, "edition_id": edition_j_id},
        "K": {"text_id": text_k_id, "edition_id": edition_k_id},
        "L": {"text_id": text_l_id, "edition_id": edition_l_id},
        "M": {"text_id": text_m_id, "edition_id": edition_m_id},
        "N": {"text_id": text_n_id, "edition_id": edition_n_id},
    }

class TestGetSegmentRelationV2:

    def _create_category(self, test_database) -> str:
        """Create a test category"""
        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        return category_id
    
    def _create_person(self, test_database, test_person_data) -> str:
        """Create a test person"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
        return person_id

    def _create_expression_and_manifestation_for_text_A(
        self,
        client,
        test_expression_data,
        category_id,
        person_id
    ) -> tuple[str, str]:
        """Create a test expression"""

        expression_data = test_expression_data
        expression_data["title"] = {"bo": "Bo root expression"}
        expression_data["language"] = "bo"
        expression_data["category_id"] = category_id
        expression_data["bdrc"] = "W123456"
        expression_data["wiki"] = "Q123456"
        expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionInput.model_validate(expression_data)
        
        expression_response = client.post("/v2/texts", data=json.dumps(expression.model_dump(mode="json")), content_type="application/json")
        
        assert expression_response.status_code == 201
        data = json.loads(expression_response.data)
        expression_id = data["id"]

        manifestation_data = {
            "metadata": {
                "wiki": "Q1",
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
        manifestation = EditionRequestModel.model_validate(manifestation_data)
        manifestation_response = client.post(f"/v2/texts/{expression_id}/editions", data=json.dumps(manifestation.model_dump(mode="json")), content_type="application/json")
        
        assert manifestation_response.status_code == 201
        data = json.loads(manifestation_response.data)
        manifestation_id = data["id"]
        
        return expression_id, manifestation_id

    def _create_text_B_translation_target_text_A(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test translation"""

        translation_request = {
            "language": "en",
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
            f"/v2/editions/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )

        assert translation_response.status_code == 201
        data = translation_response.get_json()
        translation_text_id = data["text_id"]
        translation_edition_id = data["edition_id"]
        return translation_text_id, translation_edition_id

    def _create_text_C_commentary_target_text_A(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test commentary"""

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
            f"/v2/editions/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )

        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        return data["text_id"], data["edition_id"]
    
    def _create_text_D_commentary_target_text_B(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test commentary"""

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
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }

        commentary = AlignedTextRequestModel.model_validate(commentary_request)
        commentary_response = client.post(
            f"/v2/editions/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )

        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        return data["text_id"], data["edition_id"]

    def _create_text_E_translation_target_text_A(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test translation"""
        translation_request = {
            "language": "en",
            "content": "This is the translated text content",
            "title": "E. En translation with target A",
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
            f"/v2/editions/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        data = translation_response.get_json()
        return data["text_id"], data["edition_id"]

    def _create_text_F_commentary_target_text_E(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test commentary"""
        commentary_request = {
            "language": "en",
            "content": "This is the translated text content",
            "title": "F. En commentary with target E",
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
        commentary = AlignedTextRequestModel.model_validate(commentary_request)
        commentary_response = client.post(
            f"/v2/editions/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )
        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        return data["text_id"], data["edition_id"]
    
    def _create_text_G_translation_target_text_C(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test translation"""
        translation_request = {
            "language": "zh",
            "content": "This is the translated text content",
            "title": "G. Zh translation with target C",
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
            f"/v2/editions/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        data = translation_response.get_json()
        return data["text_id"], data["edition_id"]

    def _create_text_H_commentary_target_text_C(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test commentary"""

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
                        "end": 54
                    },
                    "index": 0,
                },
                {
                    "span": {
                        "start": 54,
                        "end": 71
                    },
                    "index": 1,
                }
                
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 15
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 15,
                        "end": 20
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
            f"/v2/editions/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )
        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        return data["text_id"], data["edition_id"]

    def _create_text_I_commentary_target_text_G(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test commentary"""
        commentary_request = {
            "language": "zh",
            "content": "This is the commentary text content",
            "title": "I. Zh commentary with target G",
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
            f"/v2/editions/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )
        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        return data["text_id"], data["edition_id"]
    
    def _create_text_J_translation_target_text_H(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test translation"""

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
            f"/v2/editions/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        data = translation_response.get_json()
        return data["text_id"], data["edition_id"]

    def _create_text_K_translation_target_text_B(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test translation"""
        translation_request = {
            "language": "zh",
            "content": "This is the translated text content",
            "title": "K. Zh translation with target B",
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
            f"/v2/editions/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        data = translation_response.get_json()
        return data["text_id"], data["edition_id"]

    def _create_expression_and_manifestation_for_text_L(
        self,
        client,
        test_expression_data,
        category_id,
        person_id,
    ) -> tuple[str, str]:
        """Create a test expression"""
        expression_data = test_expression_data
        expression_data["title"] = {"sa": "Sa standalone translation expression"}
        expression_data["language"] = "sa"
        expression_data["target"] = "N/A"
        expression_data["bdrc"] = "W123455"
        expression_data["wiki"] = "Q123455"
        expression_data["type"] = "translation"
        expression_data["category_id"] = category_id
        expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionInput.model_validate(expression_data)
        
        expression_response = client.post(
            "/v2/texts",
            data=json.dumps(expression.model_dump(mode="json")),
            content_type="application/json",
        )
        
        assert expression_response.status_code == 201
        data = json.loads(expression_response.data)
        expression_id = data["id"]

        manifestation_data = {
            "metadata": {
                "wiki": "Q2",
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
        manifestation = EditionRequestModel.model_validate(manifestation_data)
        manifestation_response = client.post(
            f"/v2/texts/{expression_id}/editions",
            data=json.dumps(manifestation.model_dump(mode="json")),
            content_type="application/json",
        )
        
        assert manifestation_response.status_code == 201
        data = json.loads(manifestation_response.data)
        manifestation_id = data["id"]
        
        return expression_id, manifestation_id

    def _create_text_M_translation_target_text_L(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test translation"""

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
            f"/v2/editions/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        data = translation_response.get_json()
        return data["text_id"], data["edition_id"]
    
    def _create_text_N_commentary_target_text_L(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test commentary"""
        commentary_request = {
            "language": "bo",
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
            f"/v2/editions/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )
        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        return data["text_id"], data["edition_id"]

    def _create_expression_and_manifestation_for_text_O(
        self,
        client,
        test_expression_data,
        category_id,
        person_id,
    ) -> tuple[str, str]:
        """Create a test expression"""
        expression_data = test_expression_data
        expression_data["title"] = {"bo": "Bo standalone commentary expression"}
        expression_data["language"] = "bo"
        expression_data["target"] = "N/A"
        expression_data["bdrc"] = "W123454"
        expression_data["wiki"] = "Q123454"
        expression_data["type"] = "commentary"
        expression_data["category_id"] = category_id
        expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionInput.model_validate(expression_data)
        
        expression_response = client.post(
            "/v2/texts",
            data=json.dumps(expression.model_dump(mode="json")),
            content_type="application/json",
        )
        
        assert expression_response.status_code == 201
        data = json.loads(expression_response.data)
        expression_id = data["id"]

        manifestation_data = {
            "metadata": {
                "wiki": "Q3",
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
        manifestation = EditionRequestModel.model_validate(manifestation_data)
        manifestation_response = client.post(
            f"/v2/texts/{expression_id}/editions",
            data=json.dumps(manifestation.model_dump(mode="json")),
            content_type="application/json",
        )
        
        assert manifestation_response.status_code == 201
        data = json.loads(manifestation_response.data)
        manifestation_id = data["id"]
        
        return expression_id, manifestation_id

    def _create_text_P_commentary_target_text_O(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test commentary"""
        commentary_request = {
            "language": "en",
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
            f"/v2/editions/{target_manifestation_id}/commentary",
            json=commentary.model_dump()
        )
        assert commentary_response.status_code == 201
        data = commentary_response.get_json()
        return data["text_id"], data["edition_id"]

    def _create_text_Q_translation_target_text_O(
        self,
        client,
        category_id,
        person_id,
        target_manifestation_id
    ) -> tuple[str, str]:
        """Create a test translation"""

        translation_request = {
            "language": "en",
            "content": "This is the translated text content",
            "title": "Q. En translation with target O",
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
            f"/v2/editions/{target_manifestation_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        data = translation_response.get_json()
        return data["text_id"], data["edition_id"]

    def test_all_texts_creation(
        self,
        all_texts,
    ):
        """Ensure the autouse dataset fixture created all expected texts."""
        assert all_texts["category_id"]
        assert all_texts["person_id"]
        for key in ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N"]:
            assert all_texts[key]["text_id"]
            assert all_texts[key]["edition_id"]


    def test_transformed_segment_mapping_source_text_A(
        self,
        client,
        all_texts
    ):
        """Test segment mapping from source text A"""
        text_a_edition_id = all_texts["A"]["edition_id"]
        text_a_text_id = all_texts["A"]["text_id"]
        start = 0
        end = 7
        transformed = 'true'
        segment_mapping_response = client.get(
            f"/v2/editions/{text_a_edition_id}/segment-related?span_start={start}&span_end={end}&transform={transformed}"
        )
        assert segment_mapping_response.status_code == 200

        mapping_dict = {}
        
        for item in segment_mapping_response.get_json():
            edition_id = item["edition_metadata"]["id"]
            mapping_dict.setdefault(edition_id, []).extend(item["segments"])

        print("Texts")
        print(all_texts)

        print("Mapping dictionary")
        print(mapping_dict)

        text_c_edition_id = all_texts["C"]["edition_id"]
        assert text_c_edition_id in mapping_dict
        assert len(mapping_dict[text_c_edition_id]) == 1
        assert mapping_dict[text_c_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_c_edition_id][0]["span"]["end"] == 16

        text_b_edition_id = all_texts["B"]["edition_id"]
        assert text_b_edition_id in mapping_dict
        assert len(mapping_dict[text_b_edition_id]) == 2
        assert mapping_dict[text_b_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_b_edition_id][0]["span"]["end"] == 7
        assert mapping_dict[text_b_edition_id][1]["span"]["start"] == 7
        assert mapping_dict[text_b_edition_id][1]["span"]["end"] == 17

        text_e_edition_id = all_texts["E"]["edition_id"]
        assert text_e_edition_id in mapping_dict
        assert len(mapping_dict[text_e_edition_id]) == 1
        assert mapping_dict[text_e_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_e_edition_id][0]["span"]["end"] == 7

        text_h_edition_id = all_texts["H"]["edition_id"]
        assert text_h_edition_id in mapping_dict
        assert len(mapping_dict[text_h_edition_id]) == 2
        assert mapping_dict[text_h_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_h_edition_id][0]["span"]["end"] == 10
        assert mapping_dict[text_h_edition_id][1]["span"]["start"] == 10
        assert mapping_dict[text_h_edition_id][1]["span"]["end"] == 20

        text_g_edition_id = all_texts["G"]["edition_id"]
        assert text_g_edition_id in mapping_dict
        assert len(mapping_dict[text_g_edition_id]) == 1
        assert mapping_dict[text_g_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_g_edition_id][0]["span"]["end"] == 50

        text_d_edition_id = all_texts["D"]["edition_id"]
        assert text_d_edition_id in mapping_dict
        assert len(mapping_dict[text_d_edition_id]) == 1
        assert mapping_dict[text_d_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_d_edition_id][0]["span"]["end"] == 15

        text_k_edition_id = all_texts["K"]["edition_id"]
        assert text_k_edition_id in mapping_dict
        assert len(mapping_dict[text_k_edition_id]) == 2
        assert mapping_dict[text_k_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_k_edition_id][0]["span"]["end"] == 20
        assert mapping_dict[text_k_edition_id][1]["span"]["start"] == 20
        assert mapping_dict[text_k_edition_id][1]["span"]["end"] == 40

        text_f_edition_id = all_texts["F"]["edition_id"]
        assert text_f_edition_id in mapping_dict
        assert len(mapping_dict[text_f_edition_id]) == 3
        assert mapping_dict[text_f_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_f_edition_id][0]["span"]["end"] == 17
        assert mapping_dict[text_f_edition_id][1]["span"]["start"] == 17
        assert mapping_dict[text_f_edition_id][1]["span"]["end"] == 35
        assert mapping_dict[text_f_edition_id][2]["span"]["start"] == 35
        assert mapping_dict[text_f_edition_id][2]["span"]["end"] == 70

        text_j_edition_id = all_texts["J"]["edition_id"]
        assert text_j_edition_id in mapping_dict
        assert len(mapping_dict[text_j_edition_id]) == 4
        assert mapping_dict[text_j_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_j_edition_id][0]["span"]["end"] == 14
        assert mapping_dict[text_j_edition_id][1]["span"]["start"] == 14
        assert mapping_dict[text_j_edition_id][1]["span"]["end"] == 30
        assert mapping_dict[text_j_edition_id][2]["span"]["start"] == 30
        assert mapping_dict[text_j_edition_id][2]["span"]["end"] == 40
        assert mapping_dict[text_j_edition_id][3]["span"]["start"] == 40
        assert mapping_dict[text_j_edition_id][3]["span"]["end"] == 47

        text_i_edition_id = all_texts["I"]["edition_id"]
        assert text_i_edition_id in mapping_dict
        assert len(mapping_dict[text_i_edition_id]) == 2
        assert mapping_dict[text_i_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_i_edition_id][0]["span"]["end"] == 5
        assert mapping_dict[text_i_edition_id][1]["span"]["start"] == 5
        assert mapping_dict[text_i_edition_id][1]["span"]["end"] == 15


    def test_transformed_segment_mapping_source_text_I(
        self,
        client,
        all_texts
    ):
        """Test segment mapping from source text I"""
        text_i_edition_id = all_texts["I"]["edition_id"]
        text_i_text_id = all_texts["I"]["text_id"]
        start = 0
        end = 10
        transformed = 'true'
        segment_mapping_response = client.get(
            f"/v2/editions/{text_i_edition_id}/segment-related?span_start={start}&span_end={end}&transform={transformed}"
        )

        assert segment_mapping_response.status_code == 200
        mapping_dict = {}
        for item in segment_mapping_response.get_json():
            edition_id = item["edition_metadata"]["id"]
            mapping_dict.setdefault(edition_id, []).extend(item["segments"])
        
        text_g_edition_id = all_texts["G"]["edition_id"]
        assert text_g_edition_id in mapping_dict
        assert len(mapping_dict[text_g_edition_id]) == 3
        assert mapping_dict[text_g_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_g_edition_id][0]["span"]["end"] == 50
        assert mapping_dict[text_g_edition_id][1]["span"]["start"] == 50
        assert mapping_dict[text_g_edition_id][1]["span"]["end"] == 55
        assert mapping_dict[text_g_edition_id][2]["span"]["start"] == 55
        assert mapping_dict[text_g_edition_id][2]["span"]["end"] == 100

        text_c_edition_id = all_texts["C"]["edition_id"]
        assert text_c_edition_id in mapping_dict
        assert len(mapping_dict[text_c_edition_id]) == 4
        assert mapping_dict[text_c_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_c_edition_id][0]["span"]["end"] == 16
        assert mapping_dict[text_c_edition_id][1]["span"]["start"] == 16
        assert mapping_dict[text_c_edition_id][1]["span"]["end"] == 36
        assert mapping_dict[text_c_edition_id][2]["span"]["start"] == 36
        assert mapping_dict[text_c_edition_id][2]["span"]["end"] == 54
        assert mapping_dict[text_c_edition_id][3]["span"]["start"] == 54
        assert mapping_dict[text_c_edition_id][3]["span"]["end"] == 71

        text_h_edition_id = all_texts["H"]["edition_id"]
        assert text_h_edition_id in mapping_dict
        assert len(mapping_dict[text_h_edition_id]) == 2
        assert mapping_dict[text_h_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_h_edition_id][0]["span"]["end"] == 10
        assert mapping_dict[text_h_edition_id][1]["span"]["start"] == 10
        assert mapping_dict[text_h_edition_id][1]["span"]["end"] == 20

        text_a_edition_id = all_texts["A"]["edition_id"]
        assert text_a_edition_id in mapping_dict
        assert len(mapping_dict[text_a_edition_id]) == 4
        assert mapping_dict[text_a_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_a_edition_id][0]["span"]["end"] == 8
        assert mapping_dict[text_a_edition_id][1]["span"]["start"] == 8
        assert mapping_dict[text_a_edition_id][1]["span"]["end"] == 18
        assert mapping_dict[text_a_edition_id][2]["span"]["start"] == 18
        assert mapping_dict[text_a_edition_id][2]["span"]["end"] == 26
        assert mapping_dict[text_a_edition_id][3]["span"]["start"] == 26
        assert mapping_dict[text_a_edition_id][3]["span"]["end"] == 34

        text_j_edition_id = all_texts["J"]["edition_id"]
        assert text_j_edition_id in mapping_dict
        assert len(mapping_dict[text_j_edition_id]) == 4
        assert mapping_dict[text_j_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_j_edition_id][0]["span"]["end"] == 14
        assert mapping_dict[text_j_edition_id][1]["span"]["start"] == 14
        assert mapping_dict[text_j_edition_id][1]["span"]["end"] == 30
        assert mapping_dict[text_j_edition_id][2]["span"]["start"] == 30
        assert mapping_dict[text_j_edition_id][2]["span"]["end"] == 40
        assert mapping_dict[text_j_edition_id][3]["span"]["start"] == 40
        assert mapping_dict[text_j_edition_id][3]["span"]["end"] == 47

        text_b_edition_id = all_texts["B"]["edition_id"]
        assert text_b_edition_id in mapping_dict
        assert len(mapping_dict[text_b_edition_id]) == 4
        assert mapping_dict[text_b_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_b_edition_id][0]["span"]["end"] == 7
        assert mapping_dict[text_b_edition_id][1]["span"]["start"] == 7
        assert mapping_dict[text_b_edition_id][1]["span"]["end"] == 17
        assert mapping_dict[text_b_edition_id][2]["span"]["start"] == 17
        assert mapping_dict[text_b_edition_id][2]["span"]["end"] == 41
        assert mapping_dict[text_b_edition_id][3]["span"]["start"] == 41
        assert mapping_dict[text_b_edition_id][3]["span"]["end"] == 51

        text_e_edition_id = all_texts["E"]["edition_id"]
        assert text_e_edition_id in mapping_dict
        assert len(mapping_dict[text_e_edition_id]) == 4
        assert mapping_dict[text_e_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_e_edition_id][0]["span"]["end"] == 7
        assert mapping_dict[text_e_edition_id][1]["span"]["start"] == 7
        assert mapping_dict[text_e_edition_id][1]["span"]["end"] == 18
        assert mapping_dict[text_e_edition_id][2]["span"]["start"] == 18
        assert mapping_dict[text_e_edition_id][2]["span"]["end"] == 44
        assert mapping_dict[text_e_edition_id][3]["span"]["start"] == 44
        assert mapping_dict[text_e_edition_id][3]["span"]["end"] == 56

        text_k_edition_id = all_texts["K"]["edition_id"]
        assert text_k_edition_id in mapping_dict
        assert len(mapping_dict[text_k_edition_id]) == 3
        assert mapping_dict[text_k_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_k_edition_id][0]["span"]["end"] == 20
        assert mapping_dict[text_k_edition_id][1]["span"]["start"] == 20
        assert mapping_dict[text_k_edition_id][1]["span"]["end"] == 40
        assert mapping_dict[text_k_edition_id][2]["span"]["start"] == 40
        assert mapping_dict[text_k_edition_id][2]["span"]["end"] == 60

        text_d_edition_id = all_texts["D"]["edition_id"]
        assert text_d_edition_id in mapping_dict
        assert len(mapping_dict[text_d_edition_id]) == 3
        assert mapping_dict[text_d_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_d_edition_id][0]["span"]["end"] == 15
        assert mapping_dict[text_d_edition_id][1]["span"]["start"] == 15
        assert mapping_dict[text_d_edition_id][1]["span"]["end"] == 39
        assert mapping_dict[text_d_edition_id][2]["span"]["start"] == 39
        assert mapping_dict[text_d_edition_id][2]["span"]["end"] == 70

        text_f_edition_id = all_texts["F"]["edition_id"]
        assert text_f_edition_id in mapping_dict
        assert len(mapping_dict[text_f_edition_id]) == 4
        assert mapping_dict[text_f_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_f_edition_id][0]["span"]["end"] == 17
        assert mapping_dict[text_f_edition_id][1]["span"]["start"] == 17
        assert mapping_dict[text_f_edition_id][1]["span"]["end"] == 35
        assert mapping_dict[text_f_edition_id][2]["span"]["start"] == 35
        assert mapping_dict[text_f_edition_id][2]["span"]["end"] == 70
        assert mapping_dict[text_f_edition_id][3]["span"]["start"] == 70
        assert mapping_dict[text_f_edition_id][3]["span"]["end"] == 91


    def test_untransformed_segment_mapping_source_text_A(
        self,
        client,
        all_texts
    ):
        """Test segment mapping from source text A"""
        text_a_edition_id = all_texts["A"]["edition_id"]
        text_a_text_id = all_texts["A"]["text_id"]
        start = 0
        end = 7
        transformed = 'false'
        segment_mapping_response = client.get(
            f"/v2/editions/{text_a_edition_id}/segment-related?span_start={start}&span_end={end}&transform={transformed}"
        )
        assert segment_mapping_response.status_code == 200

        mapping_dict = {}
        for item in segment_mapping_response.get_json():
            edition_id = item["edition_metadata"]["id"]
            mapping_dict.setdefault(edition_id, []).extend(item["segments"])

        text_c_edition_id = all_texts["C"]["edition_id"]
        assert text_c_edition_id in mapping_dict
        assert len(mapping_dict[text_c_edition_id]) == 1
        assert mapping_dict[text_c_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_c_edition_id][0]["span"]["end"] == 16

        text_b_edition_id = all_texts["B"]["edition_id"]
        assert text_b_edition_id in mapping_dict
        assert len(mapping_dict[text_b_edition_id]) == 1
        assert mapping_dict[text_b_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_b_edition_id][0]["span"]["end"] == 17

        text_e_edition_id = all_texts["E"]["edition_id"]
        assert text_e_edition_id in mapping_dict
        assert len(mapping_dict[text_e_edition_id]) == 1
        assert mapping_dict[text_e_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_e_edition_id][0]["span"]["end"] == 7

        text_h_edition_id = all_texts["H"]["edition_id"]
        assert text_h_edition_id in mapping_dict
        assert len(mapping_dict[text_h_edition_id]) == 1
        assert mapping_dict[text_h_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_h_edition_id][0]["span"]["end"] == 15

        text_g_edition_id = all_texts["G"]["edition_id"]
        assert text_g_edition_id in mapping_dict
        assert len(mapping_dict[text_g_edition_id]) == 1
        assert mapping_dict[text_g_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_g_edition_id][0]["span"]["end"] == 30

        text_d_edition_id = all_texts["D"]["edition_id"]
        assert text_d_edition_id in mapping_dict
        assert len(mapping_dict[text_d_edition_id]) == 1
        assert mapping_dict[text_d_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_d_edition_id][0]["span"]["end"] == 10

        text_k_edition_id = all_texts["K"]["edition_id"]
        assert text_k_edition_id in mapping_dict
        assert len(mapping_dict[text_k_edition_id]) == 2
        assert mapping_dict[text_k_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_k_edition_id][0]["span"]["end"] == 20
        assert mapping_dict[text_k_edition_id][1]["span"]["start"] == 20
        assert mapping_dict[text_k_edition_id][1]["span"]["end"] == 40

        text_f_edition_id = all_texts["F"]["edition_id"]
        assert text_f_edition_id in mapping_dict
        assert len(mapping_dict[text_f_edition_id]) == 1
        assert mapping_dict[text_f_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_f_edition_id][0]["span"]["end"] == 40

        text_j_edition_id = all_texts["J"]["edition_id"]
        assert text_j_edition_id in mapping_dict
        assert len(mapping_dict[text_j_edition_id]) == 3
        assert mapping_dict[text_j_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_j_edition_id][0]["span"]["end"] == 30
        assert mapping_dict[text_j_edition_id][1]["span"]["start"] == 30
        assert mapping_dict[text_j_edition_id][1]["span"]["end"] == 35
        assert mapping_dict[text_j_edition_id][2]["span"]["start"] == 35
        assert mapping_dict[text_j_edition_id][2]["span"]["end"] == 47

        text_i_edition_id = all_texts["I"]["edition_id"]
        assert text_i_edition_id in mapping_dict
        assert len(mapping_dict[text_i_edition_id]) == 1
        assert mapping_dict[text_i_edition_id][0]["span"]["start"] == 0
        assert mapping_dict[text_i_edition_id][0]["span"]["end"] == 7
