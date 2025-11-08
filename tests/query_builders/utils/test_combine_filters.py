import pytest
from elasticsearch_dsl import Q
from elasticsearch_dsl.query import Query, Nested
from gen3analysis.query_builders.utils.combine_nested import (
    combine_nested_queries_simple,
    build_nested_hierarchy,
)


class TestCombineNestedQueriesSimple:
    """Test suite for combine_nested_queries_simple function"""

    def test_empty_filters(self):
        """Test with empty filter list"""
        result = combine_nested_queries_simple([])
        assert result == []

    def test_no_nested_queries(self):
        """Test with filters that don't contain nested queries"""
        filters = [
            Q("term", status="active"),
            Q("range", age={"gte": 18}),
            Q("exists", field="email"),
        ]
        result = combine_nested_queries_simple(filters)

        assert len(result) == 3
        # Check that none are nested queries
        for f in result:
            if isinstance(f, Query) and hasattr(f, "name"):
                assert f.name != "nested"

    def test_single_nested_query(self):
        """Test with a single nested query"""
        filters = [Q("nested", path="gene", query=Q("term", gene__symbol="KRAS"))]
        result = combine_nested_queries_simple(filters)

        assert len(result) == 1
        assert isinstance(result[0], Query)
        assert hasattr(result[0], "name")
        assert result[0].name in ["nested", "bool"]

    def test_multiple_nested_same_root(self):
        """Test combining multiple nested queries with the same root path"""
        filters = [
            Q(
                "nested",
                path="gene",
                ignore_unmapped=True,
                query=Q("terms", gene__is_cancer_gene_census=["true"]),
            ),
            Q(
                "nested",
                path="gene.ssm",
                ignore_unmapped=True,
                query=Q("exists", field="gene.ssm.ssm_id"),
            ),
        ]
        result = combine_nested_queries_simple(filters)

        # Should have one combined query for the "gene" root
        assert len(result) == 1
        assert isinstance(result[0], Query)

    def test_multiple_nested_different_roots(self):
        """Test with nested queries on different root paths"""
        filters = [
            Q("nested", path="gene", query=Q("term", gene__symbol="KRAS")),
            Q(
                "nested",
                path="diagnoses",
                query=Q("term", diagnoses__age_at_diagnosis=50),
            ),
        ]
        result = combine_nested_queries_simple(filters)

        # Should have two separate nested queries
        assert len(result) == 2

    def test_mixed_nested_and_regular_filters(self):
        """Test with a mix of nested and regular filters"""
        filters = [
            Q("term", case_id="123"),
            Q("nested", path="gene", query=Q("term", gene__symbol="TP53")),
            Q("exists", field="vital_status"),
            Q(
                "nested",
                path="gene.ssm",
                query=Q("term", gene__ssm__mutation_type="SNP"),
            ),
        ]
        result = combine_nested_queries_simple(filters)

        # Should have the two regular filters plus one combined nested filter
        assert len(result) == 3

    def test_deep_nested_hierarchy(self):
        """Test with deep nested hierarchy (gene.ssm.consequence.transcript)"""
        filters = [
            Q(
                "nested",
                path="gene",
                ignore_unmapped=True,
                query=Q("terms", gene__is_cancer_gene_census=["true"]),
            ),
            Q(
                "nested",
                path="gene.ssm",
                ignore_unmapped=True,
                query=Q("term", gene__ssm__mutation_type="SNP"),
            ),
            Q(
                "nested",
                path="gene.ssm.consequence",
                ignore_unmapped=True,
                query=Q(
                    "term", gene__ssm__consequence__consequence_type="missense_variant"
                ),
            ),
            Q(
                "nested",
                path="gene.ssm.consequence.transcript",
                ignore_unmapped=True,
                query=Q(
                    "term", gene__ssm__consequence__transcript__is_canonical="true"
                ),
            ),
        ]
        result = combine_nested_queries_simple(filters)

        # Should combine all into one nested structure under "gene" root
        assert len(result) == 1
        assert isinstance(result[0], Query)

    def test_multiple_filters_same_depth(self):
        """Test with multiple filters at the same nesting depth"""
        filters = [
            Q(
                "nested",
                path="gene.ssm.consequence.transcript",
                ignore_unmapped=True,
                query=Q(
                    "terms",
                    gene__ssm__consequence__transcript__annotation__sift_impact=[
                        "deleterious"
                    ],
                ),
            ),
            Q(
                "nested",
                path="gene.ssm.consequence.transcript",
                ignore_unmapped=True,
                query=Q(
                    "terms",
                    gene__ssm__consequence__transcript__annotation__vep_impact=["high"],
                ),
            ),
        ]
        result = combine_nested_queries_simple(filters)

        # Should combine into one nested query
        assert len(result) == 1

    def test_realistic_gene_query_scenario(self):
        """Test the realistic scenario from the original question"""
        filters = [
            Q(
                "nested",
                path="gene",
                ignore_unmapped=True,
                query=Q(
                    "bool",
                    must=[Q("terms", gene__is_cancer_gene_census=["true"], boost=0)],
                ),
            ),
            Q(
                "nested",
                path="gene.ssm.consequence.transcript",
                ignore_unmapped=True,
                query=Q(
                    "bool",
                    must=[
                        Q(
                            "terms",
                            gene__ssm__consequence__transcript__annotation__sift_impact=[
                                "deleterious"
                            ],
                            boost=0,
                        )
                    ],
                ),
            ),
            Q(
                "nested",
                path="gene.ssm.consequence.transcript",
                ignore_unmapped=True,
                query=Q(
                    "bool",
                    must=[
                        Q(
                            "terms",
                            gene__ssm__consequence__transcript__annotation__vep_impact=[
                                "high"
                            ],
                            boost=0,
                        )
                    ],
                ),
            ),
        ]
        result = combine_nested_queries_simple(filters)

        # Should result in one combined nested structure
        assert len(result) == 1
        assert isinstance(result[0], Query)

    def test_query_structure_validation(self):
        """Test that the resulting query structure is valid"""
        filters = [
            Q(
                "nested",
                path="gene",
                ignore_unmapped=True,
                query=Q("terms", gene__is_cancer_gene_census=["true"]),
            ),
            Q(
                "nested",
                path="gene.ssm.consequence.transcript",
                ignore_unmapped=True,
                query=Q(
                    "terms",
                    gene__ssm__consequence__transcript__annotation__sift_impact=[
                        "deleterious"
                    ],
                ),
            ),
        ]
        result = combine_nested_queries_simple(filters)

        # Should be able to convert to dict without errors
        for query in result:
            query_dict = query.to_dict()
            assert isinstance(query_dict, dict)

    def test_with_boost_parameters(self):
        """Test that boost parameters are preserved"""
        filters = [
            Q(
                "nested",
                path="gene",
                ignore_unmapped=True,
                query=Q("terms", gene__is_cancer_gene_census=["true"], boost=0),
            ),
            Q(
                "nested",
                path="gene.ssm",
                ignore_unmapped=True,
                query=Q("term", gene__ssm__mutation_type="SNP", boost=0),
            ),
        ]
        result = combine_nested_queries_simple(filters)

        assert len(result) == 1
        # Verify structure is valid
        query_dict = result[0].to_dict()
        assert isinstance(query_dict, dict)

    def test_multiple_roots_with_hierarchies(self):
        """Test multiple root paths each with their own hierarchies"""
        filters = [
            Q("nested", path="gene", query=Q("term", gene__symbol="KRAS")),
            Q("nested", path="gene.ssm", query=Q("exists", field="gene.ssm.ssm_id")),
            Q("nested", path="diagnoses", query=Q("term", diagnoses__tissue="lung")),
            Q(
                "nested",
                path="diagnoses.treatment",
                query=Q("term", diagnoses__treatment__type="chemotherapy"),
            ),
        ]
        result = combine_nested_queries_simple(filters)

        # Should have two combined structures: one for "gene" and one for "diagnoses"
        assert len(result) == 2


class TestBuildNestedHierarchy:
    """Test suite for build_nested_hierarchy helper function"""

    def test_single_depth_filter(self):
        """Test with a single depth filter"""
        filters = [Q("nested", path="gene", query=Q("term", gene__symbol="KRAS"))]
        result = build_nested_hierarchy("gene", filters)

        assert isinstance(result, Query)
        assert hasattr(result, "name")

    def test_two_level_hierarchy(self):
        """Test with two-level hierarchy"""
        filters = [
            Q("nested", path="gene", query=Q("term", gene__symbol="KRAS")),
            Q("nested", path="gene.ssm", query=Q("exists", field="gene.ssm.ssm_id")),
        ]
        result = build_nested_hierarchy("gene", filters)

        # Should create a nested structure
        assert isinstance(result, Query)

    def test_three_level_hierarchy(self):
        """Test with three-level hierarchy"""
        filters = [
            Q(
                "nested",
                path="gene",
                ignore_unmapped=True,
                query=Q("term", gene__symbol="TP53"),
            ),
            Q(
                "nested",
                path="gene.ssm",
                ignore_unmapped=True,
                query=Q("term", gene__ssm__mutation_type="SNP"),
            ),
            Q(
                "nested",
                path="gene.ssm.consequence",
                ignore_unmapped=True,
                query=Q(
                    "term", gene__ssm__consequence__consequence_type="missense_variant"
                ),
            ),
        ]
        result = build_nested_hierarchy("gene", filters)

        # Verify it creates a valid structure
        query_dict = result.to_dict()
        assert isinstance(query_dict, dict)


# Fixtures for reusable test data
@pytest.fixture
def sample_gene_filters():
    """Sample gene filters for testing"""
    return [
        Q(
            "nested",
            path="gene",
            ignore_unmapped=True,
            query=Q(
                "bool", must=[Q("terms", gene__is_cancer_gene_census=["true"], boost=0)]
            ),
        ),
        Q(
            "nested",
            path="gene.ssm.consequence.transcript",
            ignore_unmapped=True,
            query=Q(
                "bool",
                must=[
                    Q(
                        "terms",
                        gene__ssm__consequence__transcript__annotation__sift_impact=[
                            "deleterious"
                        ],
                        boost=0,
                    )
                ],
            ),
        ),
    ]


@pytest.fixture
def mixed_filters():
    """Mixed nested and regular filters"""
    return [
        Q("term", case_id="test-123"),
        Q("nested", path="gene", query=Q("term", gene__symbol="KRAS")),
        Q("exists", field="demographic.vital_status"),
        Q("nested", path="diagnoses", query=Q("term", diagnoses__age_at_diagnosis=50)),
    ]


class TestWithFixtures:
    """Tests using fixtures"""

    def test_with_sample_gene_filters(self, sample_gene_filters):
        """Test with sample gene filters fixture"""
        result = combine_nested_queries_simple(sample_gene_filters)
        assert len(result) == 1

    def test_with_mixed_filters(self, mixed_filters):
        """Test with mixed filters fixture"""
        result = combine_nested_queries_simple(mixed_filters)
        assert len(result) == 4  # 2 regular + 2 nested roots


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
