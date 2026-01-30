LOAD spatial;
LOAD httpfs;
COPY (
    WITH
    -- First pass: Get all nodes with coordinates AND tags
    osm_all_nodes AS (
        SELECT
            id,
            lat,
            lon,
            tags
        FROM st_readosm('utah-260126.osm.pbf')
        WHERE kind = 'node'
    ),
    -- Filter for restaurant nodes
    osm_node_pois AS (
        SELECT
            id,
            tags->>'name' AS name,
            tags->>'addr:housenumber' AS housenumber,
            tags->>'addr:street' AS street,
            tags->>'addr:postcode' AS postcode,
            tags->>'addr:city' AS city,
            tags->>'website' AS website,
            tags->>'phone' AS phone,
            lat,
            lon,
            'node' AS kind
        FROM osm_all_nodes
        WHERE tags->>'amenity' = 'restaurant'
    ),
    -- Second pass: Get ALL ways
    osm_all_ways AS (
        SELECT
            id,
            tags,
            refs,
            'way' AS kind
        FROM st_readosm('utah-260126.osm.pbf')
        WHERE kind = 'way'
    ),
    -- Filter for restaurant ways
    osm_ways AS (
        SELECT
            id,
            tags->>'name' AS name,
            tags->>'addr:housenumber' AS housenumber,
            tags->>'addr:street' AS street,
            tags->>'addr:postcode' AS postcode,
            tags->>'addr:city' AS city,
            tags->>'website' AS website,
            tags->>'phone' AS phone,
            refs,
            kind
        FROM osm_all_ways
        WHERE tags->>'amenity' = 'restaurant'
    ),
    -- Build way geometries from node refs
    osm_way_geoms AS (
        SELECT
            w.id,
            w.name,
            w.housenumber,
            w.street,
            w.postcode,
            w.city,
            w.website,
            w.phone,
            w.kind,
            st_centroid(
                st_makeline(
                    array_agg(st_point(n.lon, n.lat) ORDER BY idx)
                )
            ) AS geom
        FROM osm_ways w,
        LATERAL unnest(w.refs) WITH ORDINALITY AS t(node_id, idx)
        JOIN osm_all_nodes n ON n.id = t.node_id
        GROUP BY w.id, w.name, w.housenumber, w.street, w.postcode,
                 w.city, w.website, w.phone, w.kind
    ),
    -- Combine nodes and ways

    SELECT name, housenumber, street, postcode, city, website, phone, kind,
            st_point(lon, lat) AS geom
    FROM osm_node_pois
    UNION ALL
    SELECT name, housenumber, street, postcode, city, website, phone, kind,
            geom
    FROM osm_way_geoms  
) TO 'utah_restaurants_combined_with_ways.parquet';
