// =============================================================================
// UNIQUE CONSTRAINTS FOR NODES WITH ID PROPERTIES
// =============================================================================

// Person nodes - each person must have a unique ID
CREATE CONSTRAINT person_id_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE;

// Expression nodes - each expression must have a unique ID
CREATE CONSTRAINT expression_id_unique IF NOT EXISTS FOR (e:Expression) REQUIRE e.id IS UNIQUE;

// Work nodes - each work must have a unique ID
CREATE CONSTRAINT work_id_unique IF NOT EXISTS FOR (w:Work) REQUIRE w.id IS UNIQUE;

// Manifestation nodes - each manifestation must have a unique ID
CREATE CONSTRAINT manifestation_id_unique IF NOT EXISTS FOR (m:Manifestation) REQUIRE m.id IS UNIQUE;

// Annotation nodes - each annotation must have a unique ID
CREATE CONSTRAINT annotation_id_unique IF NOT EXISTS FOR (a:Annotation) REQUIRE a.id IS UNIQUE;

// =============================================================================
// UNIQUE CONSTRAINTS FOR ENUM/LOOKUP NODES
// =============================================================================

// Language nodes - each language must have a unique code
CREATE CONSTRAINT language_code_unique IF NOT EXISTS FOR (l:Language) REQUIRE l.code IS UNIQUE;

// ExpressionType nodes - each expression type must have a unique name
CREATE CONSTRAINT expression_type_name_unique IF NOT EXISTS FOR (et:ExpressionType) REQUIRE et.name IS UNIQUE;

// RoleType nodes - each role type must have a unique name
CREATE CONSTRAINT role_type_name_unique IF NOT EXISTS FOR (rt:RoleType) REQUIRE rt.name IS UNIQUE;

// AnnotationType nodes - each annotation type must have a unique name
CREATE CONSTRAINT annotation_type_name_unique IF NOT EXISTS FOR (at:AnnotationType) REQUIRE at.name IS UNIQUE;

// ManifestationType nodes - each manifestation type must have a unique name
CREATE CONSTRAINT manifestation_type_name_unique IF NOT EXISTS FOR (mt:ManifestationType) REQUIRE mt.name IS UNIQUE;

// CopyrightStatus nodes - each copyright status must have a unique name
CREATE CONSTRAINT copyright_status_name_unique IF NOT EXISTS FOR (cs:CopyrightStatus) REQUIRE cs.name IS UNIQUE;

// =============================================================================
// UNIQUE CONSTRAINTS FOR EXTERNAL IDENTIFIERS (BDRC/WIKI)
// =============================================================================

// Person nodes - BDRC IDs must be unique when present
CREATE CONSTRAINT person_bdrc_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.bdrc IS UNIQUE;

// Person nodes - Wiki IDs must be unique when present
CREATE CONSTRAINT person_wiki_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.wiki IS UNIQUE;

// Expression nodes - BDRC IDs must be unique when present
CREATE CONSTRAINT expression_bdrc_unique IF NOT EXISTS FOR (e:Expression) REQUIRE e.bdrc IS UNIQUE;

// Expression nodes - Wiki IDs must be unique when present
CREATE CONSTRAINT expression_wiki_unique IF NOT EXISTS FOR (e:Expression) REQUIRE e.wiki IS UNIQUE;

// Work nodes - BDRC IDs must be unique when present
CREATE CONSTRAINT work_bdrc_unique IF NOT EXISTS FOR (w:Work) REQUIRE w.bdrc IS UNIQUE;

// Work nodes - Wiki IDs must be unique when present
CREATE CONSTRAINT work_wiki_unique IF NOT EXISTS FOR (w:Work) REQUIRE w.wiki IS UNIQUE;

// Manifestation nodes - BDRC IDs must be unique when present
CREATE CONSTRAINT manifestation_bdrc_unique IF NOT EXISTS FOR (m:Manifestation) REQUIRE m.bdrc IS UNIQUE;

// Manifestation nodes - Wiki IDs must be unique when present
CREATE CONSTRAINT manifestation_wiki_unique IF NOT EXISTS FOR (m:Manifestation) REQUIRE m.wiki IS UNIQUE;


