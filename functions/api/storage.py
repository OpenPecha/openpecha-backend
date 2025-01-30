# import firebase_admin
# from firebase_admin import firestore
# from openpecha.pecha import Pecha

# app = firebase_admin.initialize_app()
# db = firestore.client()


# def upload_text(pecha: Pecha):
#     doc_ref = db.collection("text").document("document_id")
#     doc_ref.set(
#         {"id": "document_id", "created_at": firestore.firestore.SERVER_TIMESTAMP}
#     )
