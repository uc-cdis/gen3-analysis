from typing import Dict, Optional, List, Any
from glom import glom
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.settings import settings
from gen3analysis.utils.filterEdit import (
    dot_notation_to_graphql,
    get_subfields,
)

EXPANDABLE_FIELDS = [
    "clinical_annotations.civic.gene_id",
    "clinical_annotations.civic.variant_id",
    "consequence.consequence_id",
    "consequence.transcript.aa_change",
    "consequence.transcript.aa_end",
    "consequence.transcript.aa_start",
    "consequence.transcript.annotation.amino_acids",
    "consequence.transcript.annotation.ccds",
    "consequence.transcript.annotation.cdna_position",
    "consequence.transcript.annotation.cds_end",
    "consequence.transcript.annotation.cds_length",
    "consequence.transcript.annotation.cds_position",
    "consequence.transcript.annotation.cds_start",
    "consequence.transcript.annotation.clin_sig",
    "consequence.transcript.annotation.codons",
    "consequence.transcript.annotation.dbsnp_rs",
    "consequence.transcript.annotation.dbsnp_val_status",
    "consequence.transcript.annotation.domains",
    "consequence.transcript.annotation.ensp",
    "consequence.transcript.annotation.existing_variation",
    "consequence.transcript.annotation.hgvsc",
    "consequence.transcript.annotation.hgvsp",
    "consequence.transcript.annotation.hgvsp_short",
    "consequence.transcript.annotation.polyphen_impact",
    "consequence.transcript.annotation.polyphen_score",
    "consequence.transcript.annotation.protein_position",
    "consequence.transcript.annotation.pubmed",
    "consequence.transcript.annotation.sift_impact",
    "consequence.transcript.annotation.sift_score",
    "consequence.transcript.annotation.swissprot",
    "consequence.transcript.annotation.transcript_id",
    "consequence.transcript.annotation.trembl",
    "consequence.transcript.annotation.uniparc",
    "consequence.transcript.annotation.vep_impact",
    "consequence.transcript.consequence_type",
    "consequence.transcript.gene.biotype",
    "consequence.transcript.gene.canonical_transcript_id",
    "consequence.transcript.gene.cytoband",
    "consequence.transcript.gene.external_db_ids.entrez_gene",
    "consequence.transcript.gene.external_db_ids.hgnc",
    "consequence.transcript.gene.external_db_ids.omim_gene",
    "consequence.transcript.gene.external_db_ids.uniprotkb_swissprot",
    "consequence.transcript.gene.gene_chromosome",
    "consequence.transcript.gene.gene_end",
    "consequence.transcript.gene.gene_id",
    "consequence.transcript.gene.gene_start",
    "consequence.transcript.gene.gene_strand",
    "consequence.transcript.gene.is_cancer_gene_census",
    "consequence.transcript.gene.symbol",
    "consequence.transcript.gene.synonyms",
    "consequence.transcript.is_canonical",
    "consequence.transcript.ref_seq_accession",
    "consequence.transcript.transcript_id",
    "occurrence.case.available_variation_data",
    "occurrence.case.case_id",
    "occurrence.case.consent_type",
    "occurrence.case.days_to_consent",
    "occurrence.case.days_to_index",
    "occurrence.case.demographic.age_at_index",
    "occurrence.case.demographic.age_is_obfuscated",
    "occurrence.case.demographic.cause_of_death",
    "occurrence.case.demographic.days_to_birth",
    "occurrence.case.demographic.days_to_death",
    "occurrence.case.demographic.demographic_id",
    "occurrence.case.demographic.ethnicity",
    "occurrence.case.demographic.gender",
    "occurrence.case.demographic.race",
    "occurrence.case.demographic.state",
    "occurrence.case.demographic.submitter_id",
    "occurrence.case.demographic.vital_status",
    "occurrence.case.demographic.year_of_birth",
    "occurrence.case.demographic.year_of_death",
    "occurrence.case.diagnoses.age_at_diagnosis",
    "occurrence.case.diagnoses.ajcc_clinical_m",
    "occurrence.case.diagnoses.ajcc_clinical_n",
    "occurrence.case.diagnoses.ajcc_clinical_stage",
    "occurrence.case.diagnoses.ajcc_clinical_t",
    "occurrence.case.diagnoses.ajcc_pathologic_m",
    "occurrence.case.diagnoses.ajcc_pathologic_n",
    "occurrence.case.diagnoses.ajcc_pathologic_stage",
    "occurrence.case.diagnoses.ajcc_pathologic_t",
    "occurrence.case.diagnoses.ajcc_staging_system_edition",
    "occurrence.case.diagnoses.ann_arbor_b_symptoms",
    "occurrence.case.diagnoses.ann_arbor_clinical_stage",
    "occurrence.case.diagnoses.ann_arbor_extranodal_involvement",
    "occurrence.case.diagnoses.ann_arbor_pathologic_stage",
    "occurrence.case.diagnoses.burkitt_lymphoma_clinical_variant",
    "occurrence.case.diagnoses.cause_of_death",
    "occurrence.case.diagnoses.classification_of_tumor",
    "occurrence.case.diagnoses.cog_renal_stage",
    "occurrence.case.diagnoses.colon_polyps_history",
    "occurrence.case.diagnoses.days_to_diagnosis",
    "occurrence.case.diagnoses.days_to_hiv_diagnosis",
    "occurrence.case.diagnoses.days_to_last_follow_up",
    "occurrence.case.diagnoses.days_to_last_known_disease_status",
    "occurrence.case.diagnoses.days_to_new_event",
    "occurrence.case.diagnoses.days_to_recurrence",
    "occurrence.case.diagnoses.diagnosis_id",
    "occurrence.case.diagnoses.esophageal_columnar_dysplasia_degree",
    "occurrence.case.diagnoses.esophageal_columnar_metaplasia_present",
    "occurrence.case.diagnoses.figo_stage",
    "occurrence.case.diagnoses.figo_staging_edition_year",
    "occurrence.case.diagnoses.gastric_esophageal_junction_involvement",
    "occurrence.case.diagnoses.goblet_cells_columnar_mucosa_present",
    "occurrence.case.diagnoses.hiv_positive",
    "occurrence.case.diagnoses.hpv_positive_type",
    "occurrence.case.diagnoses.hpv_status",
    "occurrence.case.diagnoses.icd_10_code",
    "occurrence.case.diagnoses.igcccg_stage",
    "occurrence.case.diagnoses.inss_stage",
    "occurrence.case.diagnoses.international_prognostic_index",
    "occurrence.case.diagnoses.iss_stage",
    "occurrence.case.diagnoses.last_known_disease_status",
    "occurrence.case.diagnoses.laterality",
    "occurrence.case.diagnoses.ldh_level_at_diagnosis",
    "occurrence.case.diagnoses.ldh_normal_range_upper",
    "occurrence.case.diagnoses.masaoka_stage",
    "occurrence.case.diagnoses.metastasis_at_diagnosis",
    "occurrence.case.diagnoses.metastasis_at_diagnosis_site",
    "occurrence.case.diagnoses.method_of_diagnosis",
    "occurrence.case.diagnoses.micropapillary_features",
    "occurrence.case.diagnoses.morphology",
    "occurrence.case.diagnoses.new_event_anatomic_site",
    "occurrence.case.diagnoses.new_event_type",
    "occurrence.case.diagnoses.pathology_details.anaplasia_present",
    "occurrence.case.diagnoses.pathology_details.anaplasia_present_type",
    "occurrence.case.diagnoses.pathology_details.bone_marrow_malignant_cells",
    "occurrence.case.diagnoses.pathology_details.breslow_thickness",
    "occurrence.case.diagnoses.pathology_details.circumferential_resection_margin",
    "occurrence.case.diagnoses.pathology_details.columnar_mucosa_present",
    "occurrence.case.diagnoses.pathology_details.dysplasia_degree",
    "occurrence.case.diagnoses.pathology_details.dysplasia_type",
    "occurrence.case.diagnoses.pathology_details.greatest_tumor_dimension",
    "occurrence.case.diagnoses.pathology_details.gross_tumor_weight",
    "occurrence.case.diagnoses.pathology_details.largest_extrapelvic_peritoneal_focus",
    "occurrence.case.diagnoses.pathology_details.lymph_node_involved_site",
    "occurrence.case.diagnoses.pathology_details.lymph_node_involvement",
    "occurrence.case.diagnoses.pathology_details.lymph_nodes_positive",
    "occurrence.case.diagnoses.pathology_details.lymph_nodes_tested",
    "occurrence.case.diagnoses.pathology_details.lymphatic_invasion_present",
    "occurrence.case.diagnoses.pathology_details.margin_status",
    "occurrence.case.diagnoses.pathology_details.metaplasia_present",
    "occurrence.case.diagnoses.pathology_details.morphologic_architectural_pattern",
    "occurrence.case.diagnoses.pathology_details.non_nodal_regional_disease",
    "occurrence.case.diagnoses.pathology_details.non_nodal_tumor_deposits",
    "occurrence.case.diagnoses.pathology_details.number_proliferating_cells",
    "occurrence.case.diagnoses.pathology_details.pathology_detail_id",
    "occurrence.case.diagnoses.pathology_details.percent_tumor_invasion",
    "occurrence.case.diagnoses.pathology_details.perineural_invasion_present",
    "occurrence.case.diagnoses.pathology_details.peripancreatic_lymph_nodes_positive",
    "occurrence.case.diagnoses.pathology_details.peripancreatic_lymph_nodes_tested",
    "occurrence.case.diagnoses.pathology_details.prostatic_chips_positive_count",
    "occurrence.case.diagnoses.pathology_details.prostatic_chips_total_count",
    "occurrence.case.diagnoses.pathology_details.prostatic_involvement_percent",
    "occurrence.case.diagnoses.pathology_details.state",
    "occurrence.case.diagnoses.pathology_details.submitter_id",
    "occurrence.case.diagnoses.pathology_details.transglottic_extension",
    "occurrence.case.diagnoses.pathology_details.tumor_largest_dimension_diameter",
    "occurrence.case.diagnoses.pathology_details.vascular_invasion_present",
    "occurrence.case.diagnoses.pathology_details.vascular_invasion_type",
    "occurrence.case.diagnoses.pregnant_at_diagnosis",
    "occurrence.case.diagnoses.primary_diagnosis",
    "occurrence.case.diagnoses.primary_gleason_grade",
    "occurrence.case.diagnoses.prior_malignancy",
    "occurrence.case.diagnoses.prior_treatment",
    "occurrence.case.diagnoses.progression_or_recurrence",
    "occurrence.case.diagnoses.residual_disease",
    "occurrence.case.diagnoses.secondary_gleason_grade",
    "occurrence.case.diagnoses.site_of_resection_or_biopsy",
    "occurrence.case.diagnoses.state",
    "occurrence.case.diagnoses.submitter_id",
    "occurrence.case.diagnoses.synchronous_malignancy",
    "occurrence.case.diagnoses.tissue_or_organ_of_origin",
    "occurrence.case.diagnoses.treatments.chemo_concurrent_to_radiation",
    "occurrence.case.diagnoses.treatments.days_to_treatment_end",
    "occurrence.case.diagnoses.treatments.days_to_treatment_start",
    "occurrence.case.diagnoses.treatments.initial_disease_status",
    "occurrence.case.diagnoses.treatments.number_of_cycles",
    "occurrence.case.diagnoses.treatments.regimen_or_line_of_therapy",
    "occurrence.case.diagnoses.treatments.state",
    "occurrence.case.diagnoses.treatments.submitter_id",
    "occurrence.case.diagnoses.treatments.therapeutic_agents",
    "occurrence.case.diagnoses.treatments.treatment_anatomic_site",
    "occurrence.case.diagnoses.treatments.treatment_dose",
    "occurrence.case.diagnoses.treatments.treatment_frequency",
    "occurrence.case.diagnoses.treatments.treatment_id",
    "occurrence.case.diagnoses.treatments.treatment_intent_type",
    "occurrence.case.diagnoses.treatments.treatment_or_therapy",
    "occurrence.case.diagnoses.treatments.treatment_outcome",
    "occurrence.case.diagnoses.treatments.treatment_type",
    "occurrence.case.diagnoses.tumor_grade",
    "occurrence.case.diagnoses.year_of_diagnosis",
    "occurrence.case.disease_type",
    "occurrence.case.exposures.alcohol_days_per_week",
    "occurrence.case.exposures.alcohol_history",
    "occurrence.case.exposures.alcohol_intensity",
    "occurrence.case.exposures.asbestos_exposure",
    "occurrence.case.exposures.cigarettes_per_day",
    "occurrence.case.exposures.exposure_id",
    "occurrence.case.exposures.pack_years_smoked",
    "occurrence.case.exposures.radon_exposure",
    "occurrence.case.exposures.state",
    "occurrence.case.exposures.submitter_id",
    "occurrence.case.exposures.tobacco_smoking_onset_year",
    "occurrence.case.exposures.tobacco_smoking_quit_year",
    "occurrence.case.exposures.tobacco_smoking_status",
    "occurrence.case.exposures.years_smoked",
    "occurrence.case.family_histories.family_history_id",
    "occurrence.case.family_histories.relationship_age_at_diagnosis",
    "occurrence.case.family_histories.relationship_gender",
    "occurrence.case.family_histories.relationship_primary_diagnosis",
    "occurrence.case.family_histories.relationship_type",
    "occurrence.case.family_histories.relative_with_cancer_history",
    "occurrence.case.family_histories.state",
    "occurrence.case.family_histories.submitter_id",
    "occurrence.case.index_date",
    "occurrence.case.lost_to_followup",
    "occurrence.case.observation.center",
    "occurrence.case.observation.input_bam_file.normal_bam_uuid",
    "occurrence.case.observation.input_bam_file.tumor_bam_uuid",
    "occurrence.case.observation.mutation_status",
    "occurrence.case.observation.normal_genotype.match_norm_seq_allele1",
    "occurrence.case.observation.normal_genotype.match_norm_seq_allele2",
    "occurrence.case.observation.observation_id",
    "occurrence.case.observation.read_depth.n_depth",
    "occurrence.case.observation.read_depth.t_alt_count",
    "occurrence.case.observation.read_depth.t_depth",
    "occurrence.case.observation.read_depth.t_ref_count",
    "occurrence.case.observation.sample.matched_norm_sample_barcode",
    "occurrence.case.observation.sample.matched_norm_sample_uuid",
    "occurrence.case.observation.sample.tumor_sample_barcode",
    "occurrence.case.observation.sample.tumor_sample_uuid",
    "occurrence.case.observation.tumor_genotype.tumor_seq_allele1",
    "occurrence.case.observation.tumor_genotype.tumor_seq_allele2",
    "occurrence.case.observation.validation.tumor_validation_allele1",
    "occurrence.case.observation.validation.tumor_validation_allele2",
    "occurrence.case.observation.validation.validation_method",
    "occurrence.case.observation.variant_calling.variant_caller",
    "occurrence.case.observation.variant_calling.variant_process",
    "occurrence.case.primary_site",
    "occurrence.case.project.dbgap_accession_number",
    "occurrence.case.project.disease_type",
    "occurrence.case.project.intended_release_date",
    "occurrence.case.project.name",
    "occurrence.case.project.primary_site",
    "occurrence.case.project.program.dbgap_accession_number",
    "occurrence.case.project.program.name",
    "occurrence.case.project.program.program_id",
    "occurrence.case.project.project_id",
    "occurrence.case.samples.preservation_method",
    "occurrence.case.samples.sample_type",
    "occurrence.case.samples.specimen_type",
    "occurrence.case.samples.tissue_type",
    "occurrence.case.samples.tumor_descriptor",
    "occurrence.case.state",
    "occurrence.case.submitter_id",
    "occurrence.case.tissue_source_site.bcr_id",
    "occurrence.case.tissue_source_site.code",
    "occurrence.case.tissue_source_site.name",
    "occurrence.case.tissue_source_site.project",
    "occurrence.case.tissue_source_site.tissue_source_site_id",
    "occurrence.occurrence_id",
]

DEFAULT_FIELDS = [
    "start_position",
    "gene_aa_change",
    "reference_allele",
    "ncbi_build",
    "cosmic_id",
    "mutation_subtype",
    "mutation_type",
    "chromosome",
    "genomic_dna_change",
    "tumor_allele",
    "end_position",
    "ssm_id",
]


def process_item_fields(fields):
    results = ""
    for field in fields:
        results += dot_notation_to_graphql(field)
    return results


async def ssms_query(
    gen3_graphql_client: GuppyGQLClient,
    filter=None,
    fields=None,
    expand=None,
    size=1,
    offset=0,
    access_token: Optional[str] = None,
):
    if filter is None:
        filter = {}
    field_snippets: List[str] = []

    if not fields:
        field_snippets.extend(DEFAULT_FIELDS)

    if expand:
        for field in expand:
            expand_fields = get_subfields(EXPANDABLE_FIELDS, field)
            for f in expand_fields:
                field_snippets.append(dot_notation_to_graphql(f))

    seen = set()
    query_fields = " ".join(x for x in field_snippets if not (x in seen or seen.add(x)))
    query = f"""
    query ssmsQuery($filter: JSON, $first: Int, $offset: Int, $accessibility: Accessibility) {{
    {settings.ssm_centric_gql}(first: $first, offset:$offset, filter:$filter, accessibility:$accessibility) {{
            {query_fields}
            }}
    {settings.ssm_centric_agg_gql} {{ {settings.SSM_CENTRIC_INDEX}(filter:$filter, accessibility:$accessibility) {{
            _totalCount
         }}
    }}
   }}"""

    data = await gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={
            "filter": filter,
            "size": size,
            "offset": offset,
            "accessibility": "accessible",
        },
    )

    hits = glom(data, f"data.{settings.ssm_centric_gql}")
    total = glom(
        data,
        f"data.{settings.ssm_centric_agg_gql}.{settings.SSM_CENTRIC_INDEX}._totalCount",
    )
    return {
        "data": hits,
        "pagination": {"total": total, "size": size, "offset": offset},
    }


async def ssms_id_query(
    gen3_graphql_client: GuppyGQLClient,
    id: str,
    fields: Optional[List[str]] = None,
    expand: Optional[List[str]] = None,
    access_token: Optional[str] = None,
) -> Dict[str, Any]:
    field_snippets: List[str] = []

    if not fields:
        # Ensure DEFAULT_FIELDS is defined/imported appropriately
        field_snippets.extend(DEFAULT_FIELDS)
    else:
        # Convert each requested field
        for f in fields:
            field_snippets.append(dot_notation_to_graphql(f))

    # Handle expand
    if expand:
        for field in expand:
            expand_fields = get_subfields(EXPANDABLE_FIELDS, field)
            for f in expand_fields:
                field_snippets.append(dot_notation_to_graphql(f))

    seen = set()
    query_fields = " ".join(x for x in field_snippets if not (x in seen or seen.add(x)))

    # Use the correct SSM index; adjust if your schema uses a different name
    index_name = settings.SSM_CENTRIC_INDEX

    query = f"""
    query ssmsQuery($filter: JSON, $accessibility: Accessibility) {{
    {settings.ssm_centric_gql}(filter:$filter, first:1, offset:0, accessibility:$accessibility) {{
         {query_fields}
         }}
    }}"""

    return await gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={"filter": {"in": {"ssm_id": [id]}}},
    )
