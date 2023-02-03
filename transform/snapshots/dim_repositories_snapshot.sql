{% snapshot dim_repositories_snapshot %}

    {{ config(
            target_schema=var('schema'),
            unique_key='id',
            strategy='timestamp',
            updated_at='_sdc_batched_at'
        )
    }}

    with repositories as (
        select * from {{ source('github_source', 'repositories') }}
    )

    select * from repositories
{% endsnapshot %}