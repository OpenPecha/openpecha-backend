import logging

from flask import Blueprint, Response, jsonify
from neo4j_database import Neo4JDatabase
from exceptions import InvalidRequest

relation_bp = Blueprint("relation", __name__)

logger = logging.getLogger(__name__)


@relation_bp.route("/expressions/<string:expression_id>", methods=["GET"], strict_slashes=False)
def get_expression_relations(expression_id: str) -> tuple[Response, int]:
    
    
    response: dict = _get_relation_for_an_expression(expression_id=expression_id)

    return jsonify(response), 200

def _get_relation_for_an_expression(expression_id: str) -> dict:

    relationship = _get_expression_relations(expression_id)
    response = {}

    for key, value in relationship.items():
        if value is None:
            continue
        if response.get(value, None) is None:
            response[value] = []
        response[value].append(key)
    
    return response

def _get_relation_according_to_relation_rule(relation: str) -> str:
    rules = {
        "ROOT-ROOT": "SIBLING_ROOT",
        "ROOT-TRANSLATION": "ROOT",
        "TRANSLATION-TRANSLATION": "TRANSLATION",
        "TRANSLATION-ROOT": "SIBLING_ROOT",
        "ROOT-COMMENTARY": "SIBLING_COMMENTARY",
        "SIBLING_COMMENTARY-TRANSLATION": "SIBLING_COMMENTARY",
        "TRANSLATION-COMMENTARY": "COMMENTARY",
        "SIBLING_ROOT-TRANSLATION": "SIBLING_ROOT",
        "SIBLING_ROOT-ROOT": "SIBLING_ROOT",
        "COMMENTARY-TRANSLATION": "COMMENTARY",
        "ROOT-ROOT": "SIBLING_ROOT",
        "COMMENTARY-ROOT": "SIBLING_COMMENTARY",
        "COMMENTARY-COMMENTARY": "SIBLING_COMMENTARY",
        "SIBLING_ROOT-COMMENTARY": "SIBLING_COMMENTARY",
        "SIBLING_COMMENTARY-COMMENTARY": "SIBLING_COMMENTARY"
    }

    return rules.get(relation, None)


def _get_expression_relations(expression_id: str):
    db = Neo4JDatabase()
    expression_relations = db.get_all_expression_relations()
    if expression_id not in expression_relations:
        raise InvalidRequest(f"Expression with ID {expression_id} not found")
    relation_dict = {}
    queue = []
    explored_expression = set()

    relation_dict[expression_id] = None
    queue.append(expression_id)

    while queue:
        current_id = queue.pop(0)
        
        # Skip if already explored (prevents infinite loops)
        if current_id in explored_expression:
            continue
        # Mark as explored BEFORE processing neighbors
        explored_expression.add(current_id)
        logger.info(f"Exploring expression: {current_id}")
        
        # Process all relations for current node
        for relation in expression_relations[current_id]:
            other_id = relation.get("otherId")
            
            # Skip if the related node has already been explored
            if other_id in explored_expression:
                continue
            
            if relation.get("type") == "TRANSLATION_OF":
                new_relation = "TRANSLATION"
                pre_relation = relation_dict.get(current_id)
                if pre_relation is None:
                    relation_dict[other_id] = new_relation
                else:
                    combined_relation = f"{pre_relation}-{new_relation}"
                    final_relation = _get_relation_according_to_relation_rule(combined_relation)
                    relation_dict[other_id] = final_relation
                # Only add to queue if not already explored
                if other_id not in explored_expression:
                    queue.append(other_id)
                    
            elif relation.get("type") == "COMMENTARY_OF":
                new_relation = None
                if relation.get("direction") == "in":
                    new_relation = "COMMENTARY"
                elif relation.get("direction") == "out":
                    new_relation = "ROOT"
                pre_relation = relation_dict.get(current_id)
                if pre_relation is None:
                    relation_dict[other_id] = new_relation
                else:
                    combined_relation = f"{pre_relation}-{new_relation}"
                    final_relation = _get_relation_according_to_relation_rule(combined_relation)
                    relation_dict[other_id] = final_relation
                # Only add to queue if not already explored
                if other_id not in explored_expression:
                    queue.append(other_id)
    return relation_dict