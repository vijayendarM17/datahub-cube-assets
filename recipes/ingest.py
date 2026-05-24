#!/usr/bin/env python3
"""
Reads cube YAML files and pushes metadata to DataHub.
Automatically detects lineage changes on every run and stores version history.
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
)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

GMS_URL = os.environ.get('DATAHUB_GMS_URL', 'http://localhost:8080')
TOKEN   = os.environ.get('DATAHUB_TOKEN', '')

def platform_urn(platform):
    return f"urn:li:dataPlatform:{platform}"

def dataset_urn(platform, name, env="PROD"):
    return f"urn:li:dataset:({platform_urn(platform)},{name},{env})"

def domain_urn(name):
    return f"urn:li:domain:{name.lower().replace(' ', '_')}"

def field_type(type_str):
    if type_str == "number":
        return SchemaFieldDataTypeClass(type=NumberTypeClass())
    return SchemaFieldDataTypeClass(type=StringTypeClass())

# ---------------------------------------------------------------------------
# Lineage version tracking
# ---------------------------------------------------------------------------

def _fetch_dataset(urn):
    """Fetch dataset JSON from DataHub OpenAPI v3."""
    headers = {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}
    try:
        r = requests.get(
            f"{GMS_URL}/openapi/v3/entity/dataset/{urllib.parse.quote(urn, safe='')}",
            headers=headers,
            timeout=10,
        )
        return r.json() if r.ok else {}
    except Exception:
        return {}


def compute_version_changes(cube_urn, new_upstream_urns, new_columns):
    """
    Compare incoming upstreams AND columns with what DataHub currently has.
    Detects: upstream tables added/removed, columns added/removed, column type changed.

    new_columns: list of {"name": str, "type": str}
    Returns (version_number: int, versions_json: str, changed: bool)
    """
    data = _fetch_dataset(cube_urn)
    props = (
        data.get('datasetProperties', {})
            .get('value', {})
            .get('customProperties', {})
    )

    current_version = int(props.get('lineage_version', '0'))
    try:
        versions = json.loads(props.get('lineage_versions', '[]'))
    except Exception:
        versions = []

    # --- Upstream diff ---
    current_ups_raw = (
        data.get('upstreamLineage', {})
            .get('value', {})
            .get('upstreams', [])
    )
    current_ups = set(u['dataset'] for u in current_ups_raw)
    new_ups = set(new_upstream_urns)
    ups_added   = sorted(new_ups - current_ups)
    ups_removed = sorted(current_ups - new_ups)

    # --- Column diff ---
    current_fields = (
        data.get('schemaMetadata', {})
            .get('value', {})
            .get('fields', [])
    )
    current_cols = {f['fieldPath']: f.get('nativeDataType', '') for f in current_fields}
    new_cols     = {col['name']: col['type'] for col in new_columns}

    # Pull explicit renames declared in YAML: {"new_name": "old_name"}
    explicit_renames = {
        col['name']: col['renamed_from']
        for col in new_columns
        if col.get('renamed_from')
    }
    renamed_old_names = set(explicit_renames.values())

    # Additions / removals exclude explicitly renamed columns
    cols_added   = sorted(n for n in (set(new_cols) - set(current_cols)) if n not in explicit_renames)
    cols_removed = sorted(o for o in (set(current_cols) - set(new_cols)) if o not in renamed_old_names)
    cols_renamed = sorted(
        [{"from": old, "to": new} for new, old in explicit_renames.items() if old in current_cols],
        key=lambda x: x['from'],
    )
    cols_type_changed = sorted(
        [
            {"col": col, "from": current_cols[col], "to": new_cols[col]}
            for col in set(current_cols) & set(new_cols)
            if current_cols[col] != new_cols[col]
        ],
        key=lambda x: x['col'],
    )

    changed = bool(
        current_version == 0
        or ups_added or ups_removed
        or cols_added or cols_removed or cols_renamed or cols_type_changed
    )

    if changed:
        new_version = current_version + 1
        entry = {
            "v":                 new_version,
            "ts":                datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            "ups":               sorted(new_ups),
            "ups_added":         ups_added,
            "ups_removed":       ups_removed,
            "cols_added":        cols_added,
            "cols_removed":      cols_removed,
            "cols_renamed":      cols_renamed,
            "cols_type_changed": cols_type_changed,
        }
        versions.append(entry)
        return new_version, json.dumps(versions), True

    return current_version, json.dumps(versions), False


# ---------------------------------------------------------------------------
# Core ingestion
# ---------------------------------------------------------------------------

def ingest_cube(cube_yaml_path):
    with open(cube_yaml_path) as f:
        cube = yaml.safe_load(f)

    emitter = DatahubRestEmitter(gms_server=GMS_URL, token=TOKEN)

    sf_tables = cube.get('snowflake_tables') or [cube['snowflake_table']]

    # 1. Domain
    d_urn = domain_urn(cube['domain'])
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=d_urn,
        aspect=DomainPropertiesClass(name=cube['domain'], description=f"Domain for {cube['domain']}")
    ))
    print(f"✅ Domain: {cube['domain']}")

    # 2. Snowflake tables
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
                    version=0,
                    hash="",
                    platformSchema={"com.linkedin.schema.OtherSchema": {"rawSchema": ""}},
                    fields=table_cols
                )
            ))
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=sf_urn,
            aspect=DomainsClass(domains=[d_urn])
        ))
        print(f"✅ Snowflake table: {sf_table}")

    # 3. Cube dataset URN (name no longer encodes version — just domain.name)
    cube_name = f"{cube['domain'].lower().replace(' ', '_')}.{cube['name'].lower().replace(' ', '_')}"
    cube_urn = dataset_urn("cube", cube_name)

    cube_columns = [
        SchemaFieldClass(
            fieldPath=col['name'],
            type=field_type(col['type']),
            nativeDataType=col['type'],
            description=col.get('description', '')
        ) for col in cube['columns']
    ]

    # 4. Detect all changes (upstreams + columns) and compute version
    new_upstream_urns = list(sf_urns.values())
    new_columns = [
        {"name": col['name'], "type": col['type'], "renamed_from": col.get('renamed_from')}
        for col in cube['columns']
    ]
    lineage_version, versions_json, changed = compute_version_changes(
        cube_urn, new_upstream_urns, new_columns
    )

    if changed:
        v = json.loads(versions_json)[-1]
        print(f"🔄 Changes detected → version {lineage_version}")
        if v.get('ups_added'):    print(f"   + Upstreams added:    {', '.join(v['ups_added'])}")
        if v.get('ups_removed'):  print(f"   - Upstreams removed:  {', '.join(v['ups_removed'])}")
        if v.get('cols_added'):   print(f"   + Columns added:      {', '.join(v['cols_added'])}")
        if v.get('cols_removed'): print(f"   - Columns removed:    {', '.join(v['cols_removed'])}")
        for r in v.get('cols_renamed', []):  print(f"   ↪ Column renamed:     {r['from']} → {r['to']}")
        for tc in v.get('cols_type_changed', []):
            print(f"   ~ Type changed:       {tc['col']}  {tc['from']} → {tc['to']}")
    else:
        print(f"✔  No changes (version {lineage_version})")

    # 5. Emit cube dataset properties (with version tracking in customProperties)
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=cube_urn,
        aspect=DatasetPropertiesClass(
            name=cube['name'],
            description=cube['description'],
            customProperties={
                "cube_type":         "dimension",
                "lineage_version":   str(lineage_version),
                "lineage_versions":  versions_json,
            }
        )
    ))
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=cube_urn,
        aspect=SchemaMetadataClass(
            schemaName=cube['name'],
            platform=platform_urn("cube"),
            version=0,
            hash="",
            platformSchema={"com.linkedin.schema.OtherSchema": {"rawSchema": ""}},
            fields=cube_columns
        )
    ))
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=cube_urn,
        aspect=DomainsClass(domains=[d_urn])
    ))
    print(f"✅ Cube: {cube['name']}")

    # 6. Column-level lineage
    fine_grained = []
    for col in cube['columns']:
        src_table = col.get('source_table', sf_tables[0])
        src_urn   = sf_urns.get(src_table, dataset_urn("snowflake", src_table))
        fine_grained.append(FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[f"urn:li:schemaField:({src_urn},{col['mapped_to']})"],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            downstreams=[f"urn:li:schemaField:({cube_urn},{col['name']})"]
        ))

    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=cube_urn,
        aspect=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=urn, type=DatasetLineageTypeClass.TRANSFORMED)
                for urn in sf_urns.values()
            ],
            fineGrainedLineages=fine_grained
        )
    ))
    print(f"✅ Lineage: {cube['name']} → {', '.join(sf_tables)}")
    print(f"\n🎉 Done! View: http://localhost:9002/dataset/{urllib.parse.quote(cube_urn)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 ingest.py <path-to-cube-yaml>")
        sys.exit(1)
    ingest_cube(sys.argv[1])
