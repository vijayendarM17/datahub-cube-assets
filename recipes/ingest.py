#!/usr/bin/env python3
"""
Reads cube YAML files and pushes metadata to DataHub.
Uses DataHub native versioning: each detected change creates a new versioned entity.
v1, v2, v3... appear in the DataHub Version dropdown.
Clicking v2 in the dropdown shows the Lineage tab with v2's upstream tables.
Usage: python3 ingest.py <path-to-cube-yaml>
"""
import os
import sys
import json
import datetime
import urllib.parse
import yaml
import requests
from dotenv import load_dotenv
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    NumberTypeClass,
    UpstreamLineageClass,
    UpstreamClass,
    DatasetLineageTypeClass,
    DomainsClass,
    DomainPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    VersionPropertiesClass,
    VersionSetPropertiesClass,
    VersionTagClass,
    VersioningSchemeClass,
)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

GMS_URL = os.environ.get('DATAHUB_GMS_URL', 'http://localhost:8080')
TOKEN   = os.environ.get('DATAHUB_TOKEN', '')

# ---------------------------------------------------------------------------
# URN helpers
# ---------------------------------------------------------------------------

def platform_urn(platform):
    return f"urn:li:dataPlatform:{platform}"

def dataset_urn(platform, name, env="PROD"):
    return f"urn:li:dataset:({platform_urn(platform)},{name},{env})"

def versioned_urn(platform, base_name, v, env="PROD"):
    """e.g. urn:li:dataset:(urn:li:dataPlatform:cube,vijay_domain.dim_customer.v2,PROD)"""
    return dataset_urn(platform, f"{base_name}.v{v}", env)

def version_set_urn(base_name):
    """e.g. urn:li:versionSet:(vijay_domain.dim_customer,dataset)"""
    return f"urn:li:versionSet:({base_name},dataset)"

def domain_urn(name):
    return f"urn:li:domain:{name.lower().replace(' ', '_')}"

def field_type(type_str):
    if type_str == "number":
        return SchemaFieldDataTypeClass(type=NumberTypeClass())
    return SchemaFieldDataTypeClass(type=StringTypeClass())

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _headers():
    return {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}

def _fetch_dataset(urn):
    try:
        r = requests.get(
            f"{GMS_URL}/openapi/v3/entity/dataset/{urllib.parse.quote(urn, safe='')}",
            headers=_headers(), timeout=10,
        )
        return r.json() if r.ok else {}
    except Exception:
        return {}

# ---------------------------------------------------------------------------
# Version detection
# ---------------------------------------------------------------------------

def get_current_version(platform, base_name):
    """
    Find the highest existing versioned entity (dim_customer.v1, .v2, ...).
    Returns 0 if none exist.
    """
    v = 0
    while True:
        urn  = versioned_urn(platform, base_name, v + 1)
        data = _fetch_dataset(urn)
        if data.get('datasetProperties'):
            v += 1
        else:
            break
    return v

def get_version_history(entity_urn):
    data  = _fetch_dataset(entity_urn)
    props = data.get('datasetProperties', {}).get('value', {}).get('customProperties', {})
    try:
        return json.loads(props.get('lineage_versions', '[]'))
    except Exception:
        return []

# ---------------------------------------------------------------------------
# Change detection (compare new YAML state against current entity in DataHub)
# ---------------------------------------------------------------------------

def compute_diff(current_entity_urn, new_upstream_urns, new_columns):
    data = _fetch_dataset(current_entity_urn)

    current_ups_raw = data.get('upstreamLineage', {}).get('value', {}).get('upstreams', [])
    current_ups     = set(u['dataset'] for u in current_ups_raw)
    new_ups         = set(new_upstream_urns)
    ups_added       = sorted(new_ups - current_ups)
    ups_removed     = sorted(current_ups - new_ups)

    current_fields = data.get('schemaMetadata', {}).get('value', {}).get('fields', [])
    current_cols   = {f['fieldPath']: f.get('nativeDataType', '') for f in current_fields}
    new_cols       = {col['name']: col['type'] for col in new_columns}

    explicit_renames  = {col['name']: col['renamed_from'] for col in new_columns if col.get('renamed_from')}
    renamed_old_names = set(explicit_renames.values())

    cols_added        = sorted(n for n in (set(new_cols) - set(current_cols)) if n not in explicit_renames)
    cols_removed      = sorted(o for o in (set(current_cols) - set(new_cols)) if o not in renamed_old_names)
    cols_renamed      = sorted(
        [{"from": old, "to": new} for new, old in explicit_renames.items() if old in current_cols],
        key=lambda x: x['from'],
    )
    cols_type_changed = sorted(
        [{"col": c, "from": current_cols[c], "to": new_cols[c]}
         for c in set(current_cols) & set(new_cols) if current_cols[c] != new_cols[c]],
        key=lambda x: x['col'],
    )

    changed = bool(ups_added or ups_removed or cols_added or cols_removed or cols_renamed or cols_type_changed)
    return {
        "changed": changed,
        "ups_added": ups_added, "ups_removed": ups_removed,
        "cols_added": cols_added, "cols_removed": cols_removed,
        "cols_renamed": cols_renamed, "cols_type_changed": cols_type_changed,
    }

# ---------------------------------------------------------------------------
# Emit helpers
# ---------------------------------------------------------------------------

def emit_version_entity(emitter, v_urn, cube_name, cube_description, d_urn,
                         platform, cols_schema, upstream_urns, fine_grained,
                         versions_snapshot):
    """Emit all aspects for one versioned entity."""
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=v_urn,
        aspect=DatasetPropertiesClass(
            name=cube_name,
            description=cube_description,
            customProperties={
                "cube_type":        "dimension",
                "lineage_version":  str(versions_snapshot[-1]['v']),
                "lineage_versions": json.dumps(versions_snapshot),
            }
        )
    ))
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=v_urn,
        aspect=SchemaMetadataClass(
            schemaName=cube_name,
            platform=platform_urn(platform),
            version=0, hash="",
            platformSchema={"com.linkedin.schema.OtherSchema": {"rawSchema": ""}},
            fields=cols_schema
        )
    ))
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=v_urn,
        aspect=DomainsClass(domains=[d_urn])
    ))
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=v_urn,
        aspect=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED)
                for u in upstream_urns
            ],
            fineGrainedLineages=fine_grained
        )
    ))

def emit_version_props(emitter, v_urn, vs_urn, v_num, is_latest):
    # isLatest is a computed field in DataHub — do NOT set it manually.
    # It is derived from VersionSetProperties.latest pointing to this entity.
    aliases = [VersionTagClass(versionTag="latest")] if is_latest else []
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=v_urn,
        aspect=VersionPropertiesClass(
            versionSet=vs_urn,
            version=VersionTagClass(versionTag=f"v{v_num}"),
            sortId=f"v{v_num:03d}",
            aliases=aliases,
        )
    ))

# ---------------------------------------------------------------------------
# One-time migration: old single entity → versioned entities
# ---------------------------------------------------------------------------

def migrate_from_old_entity(platform, base_name, cube, emitter, d_urn):
    """
    If old dim_customer entity exists (no version suffix) and has lineage_versions,
    recreate each historical version as a proper versioned entity.
    Returns highest migrated version number, or 0 if nothing to migrate.
    """
    old_urn  = dataset_urn(platform, base_name)
    old_data = _fetch_dataset(old_urn)
    old_props = (
        old_data.get('datasetProperties', {})
                .get('value', {})
                .get('customProperties', {})
    )
    versions_raw = old_props.get('lineage_versions', '')
    if not versions_raw:
        return 0
    try:
        history = json.loads(versions_raw)
    except Exception:
        return 0
    if not history:
        return 0

    print(f"📦 Migrating {len(history)} historical versions to native DataHub versioning…")
    vs_urn = version_set_urn(base_name)

    for i, entry in enumerate(history):
        v        = entry['v']
        v_urn    = versioned_urn(platform, base_name, v)
        is_latest = (i == len(history) - 1)
        cols      = entry.get('cols', [])
        ups       = entry.get('ups', [])

        # Schema: use column names from snapshot (type defaults to string for historical)
        cols_schema = [
            SchemaFieldClass(
                fieldPath=col,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType='string',
                description=''
            )
            for col in cols
        ]

        # Fine-grained lineage: assume column name = source column name for historical versions
        fine_grained_hist = [
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[f"urn:li:schemaField:({up},{col})"],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[f"urn:li:schemaField:({v_urn},{col})"]
            )
            for col in cols
            for up in ups
        ]

        emit_version_entity(
            emitter, v_urn,
            cube['name'], cube.get('description', ''),
            d_urn, platform,
            cols_schema, ups, fine_grained_hist,
            history[:i + 1],
        )
        emit_version_props(emitter, v_urn, vs_urn, v, is_latest)
        print(f"   {'✅ v' + str(v) + ' (latest)' if is_latest else '   v' + str(v)}")

    latest_v   = history[-1]['v']
    latest_urn = versioned_urn(platform, base_name, latest_v)
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=vs_urn,
        aspect=VersionSetPropertiesClass(
            latest=latest_urn,
            versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
        )
    ))
    print(f"✅ Migration done — v{latest_v} is latest in version dropdown")
    return latest_v

# ---------------------------------------------------------------------------
# Core ingestion
# ---------------------------------------------------------------------------

def ingest_cube(cube_yaml_path):
    with open(cube_yaml_path) as f:
        cube = yaml.safe_load(f)

    emitter   = DatahubRestEmitter(gms_server=GMS_URL, token=TOKEN)
    sf_tables = cube.get('snowflake_tables') or [cube['snowflake_table']]

    # 1. Domain
    d_urn = domain_urn(cube['domain'])
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=d_urn,
        aspect=DomainPropertiesClass(name=cube['domain'], description=f"Domain for {cube['domain']}")
    ))
    print(f"✅ Domain: {cube['domain']}")

    # 2. Snowflake source tables
    sf_urns = {}
    for sf_table in sf_tables:
        sf_urn = dataset_urn("snowflake", sf_table)
        sf_urns[sf_table] = sf_urn
        table_cols = [
            SchemaFieldClass(
                fieldPath=col['mapped_to'],
                type=field_type(col['type']),
                nativeDataType=col['type'],
                description=col.get('description', '')
            )
            for col in cube['columns']
            if col.get('source_table', sf_tables[0]) == sf_table
        ]
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=sf_urn,
            aspect=DatasetPropertiesClass(
                name=sf_table.split('.')[-1],
                description=f"Snowflake table: {sf_table}"
            )
        ))
        if table_cols:
            emitter.emit(MetadataChangeProposalWrapper(
                entityUrn=sf_urn,
                aspect=SchemaMetadataClass(
                    schemaName=sf_table,
                    platform=platform_urn("snowflake"),
                    version=0, hash="",
                    platformSchema={"com.linkedin.schema.OtherSchema": {"rawSchema": ""}},
                    fields=table_cols
                )
            ))
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=sf_urn,
            aspect=DomainsClass(domains=[d_urn])
        ))
        print(f"✅ Snowflake table: {sf_table}")

    # 3. Determine base name
    base_name = (
        f"{cube['domain'].lower().replace(' ', '_')}"
        f".{cube['name'].lower().replace(' ', '_')}"
    )
    vs_urn        = version_set_urn(base_name)
    new_upstream_urns = list(sf_urns.values())
    new_columns   = [
        {"name": col['name'], "type": col['type'], "renamed_from": col.get('renamed_from')}
        for col in cube['columns']
    ]

    # 4. Find current highest version
    current_v = get_current_version("cube", base_name)

    # If no versioned entities exist yet, check for old single entity and migrate
    if current_v == 0:
        current_v = migrate_from_old_entity("cube", base_name, cube, emitter, d_urn)

    # 5. Detect changes against current latest versioned entity
    if current_v == 0:
        # Truly first run ever
        changed = True
        diff = {
            "changed": True,
            "ups_added": new_upstream_urns, "ups_removed": [],
            "cols_added": [c['name'] for c in new_columns], "cols_removed": [],
            "cols_renamed": [], "cols_type_changed": [],
        }
        prev_versions = []
        print(f"🆕 First ingestion → creating v1")
    else:
        current_urn  = versioned_urn("cube", base_name, current_v)
        diff         = compute_diff(current_urn, new_upstream_urns, new_columns)
        changed      = diff["changed"]
        prev_versions = get_version_history(current_urn)

    if not changed:
        print(f"✔  No changes detected (still at v{current_v})")
        return

    new_v   = current_v + 1
    new_urn = versioned_urn("cube", base_name, new_v)

    print(f"🔄 Change detected: v{current_v} → v{new_v}")
    if diff.get('ups_added'):    print(f"   + Upstreams added:   {', '.join(diff['ups_added'])}")
    if diff.get('ups_removed'):  print(f"   - Upstreams removed: {', '.join(diff['ups_removed'])}")
    if diff.get('cols_added'):   print(f"   + Columns added:     {', '.join(diff['cols_added'])}")
    if diff.get('cols_removed'): print(f"   - Columns removed:   {', '.join(diff['cols_removed'])}")
    for r  in diff.get('cols_renamed', []):       print(f"   ↪ Column renamed:    {r['from']} → {r['to']}")
    for tc in diff.get('cols_type_changed', []):  print(f"   ~ Type changed:      {tc['col']}  {tc['from']} → {tc['to']}")

    # 6. Build version entry
    version_entry = {
        "v":                 new_v,
        "ts":                datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        "ups":               sorted(new_upstream_urns),
        "ups_added":         diff["ups_added"],
        "ups_removed":       diff["ups_removed"],
        "cols":              sorted(c['name'] for c in new_columns),
        "cols_added":        diff["cols_added"],
        "cols_removed":      diff["cols_removed"],
        "cols_renamed":      diff["cols_renamed"],
        "cols_type_changed": diff["cols_type_changed"],
    }
    versions_list = prev_versions + [version_entry]

    # 7. Build schema for new version
    cube_columns = [
        SchemaFieldClass(
            fieldPath=col['name'],
            type=field_type(col['type']),
            nativeDataType=col['type'],
            description=col.get('description', '')
        )
        for col in cube['columns']
    ]

    # 8. Column-level lineage
    fine_grained = []
    for col in cube['columns']:
        src_table = col.get('source_table', sf_tables[0])
        src_urn   = sf_urns.get(src_table, dataset_urn("snowflake", src_table))
        fine_grained.append(FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[f"urn:li:schemaField:({src_urn},{col['mapped_to']})"],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            downstreams=[f"urn:li:schemaField:({new_urn},{col['name']})"]
        ))

    # 9. Emit new versioned entity
    emit_version_entity(
        emitter, new_urn,
        cube['name'], cube['description'],
        d_urn, "cube",
        cube_columns, new_upstream_urns, fine_grained,
        versions_list,
    )
    print(f"✅ Created cube entity: {cube['name']} v{new_v}")

    # 10. Mark old version as not-latest, new version as latest
    if current_v > 0:
        emit_version_props(
            emitter, versioned_urn("cube", base_name, current_v),
            vs_urn, current_v, is_latest=False
        )

    emit_version_props(emitter, new_urn, vs_urn, new_v, is_latest=True)

    # 11. Update version set to point to newest entity
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=vs_urn,
        aspect=VersionSetPropertiesClass(
            latest=new_urn,
            versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
        )
    ))

    print(f"\n🎉 Done! v{new_v} is now latest.")
    print(f"   View: http://localhost:9002/dataset/{urllib.parse.quote(new_urn)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 ingest.py <path-to-cube-yaml>")
        sys.exit(1)
    ingest_cube(sys.argv[1])
