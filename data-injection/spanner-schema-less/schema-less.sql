CREATE TABLE GraphNode (
  id STRING(MAX) NOT NULL,
  label STRING(MAX) NOT NULL,
  properties JSON
) PRIMARY KEY (id);

CREATE TABLE GraphEdge (
  id STRING(MAX) NOT NULL,
  dest_id STRING(MAX) NOT NULL,
  edge_id STRING(MAX) NOT NULL,
  label STRING(MAX) NOT NULL,
  properties JSON
) PRIMARY KEY (id, dest_id, edge_id),
  INTERLEAVE IN PARENT GraphNode;

CREATE PROPERTY GRAPH SchemaLessGraph
  NODE TABLES (
    GraphNode
      DYNAMIC LABEL (label)
      DYNAMIC PROPERTIES (properties)
  )
  EDGE TABLES (
    GraphEdge
      SOURCE KEY (id) REFERENCES GraphNode(id)
      DESTINATION KEY (dest_id) REFERENCES GraphNode(id)
      DYNAMIC LABEL (label)
      DYNAMIC PROPERTIES (properties)
  );