from pcca.collectors.linkedin_utils import (
    build_linkedin_activity_url,
    is_opaque_linkedin_member_id,
    normalize_linkedin_source_id,
)


def test_linkedin_activity_url_builder_is_prefix_idempotent() -> None:
    assert (
        build_linkedin_activity_url("in/ACoAAA2WrzMBW0tblYjqmElLdB695E8tu_ZWxqg")
        == "https://www.linkedin.com/in/ACoAAA2WrzMBW0tblYjqmElLdB695E8tu_ZWxqg/recent-activity/all/"
    )
    assert (
        build_linkedin_activity_url("ACoAAA2WrzMBW0tblYjqmElLdB695E8tu_ZWxqg")
        == "https://www.linkedin.com/in/ACoAAA2WrzMBW0tblYjqmElLdB695E8tu_ZWxqg/recent-activity/all/"
    )
    assert (
        build_linkedin_activity_url("https://www.linkedin.com/in/boris-cherny/recent-activity/all/")
        == "https://www.linkedin.com/in/boris-cherny/recent-activity/all/"
    )
    assert (
        build_linkedin_activity_url("company/openai")
        == "https://www.linkedin.com/company/openai/posts/"
    )


def test_linkedin_source_normalization_and_opaque_detection() -> None:
    assert normalize_linkedin_source_id("https://www.linkedin.com/in/boris-cherny/") == "in/boris-cherny"
    assert normalize_linkedin_source_id("/company/openai/posts/") == "company/openai"
    assert is_opaque_linkedin_member_id("in/ACoAAA2WrzMBW0tblYjqmElLdB695E8tu_ZWxqg")
    assert not is_opaque_linkedin_member_id("in/boris-cherny")
