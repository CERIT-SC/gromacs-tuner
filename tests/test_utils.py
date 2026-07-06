import pytest

from api.utils import AMBER_FORBIDDEN_FLAGS, GMX_FORBIDDEN_FLAGS, sanitize_extra_args


class TestSanitizeExtraArgs:
    def test_empty_string_returns_empty(self) -> None:
        assert sanitize_extra_args("", GMX_FORBIDDEN_FLAGS) == ""

    def test_valid_args_pass_through(self) -> None:
        result = sanitize_extra_args("-ntmpi 2", GMX_FORBIDDEN_FLAGS)
        assert result == "-ntmpi 2"

    def test_forbidden_characters_raise(self) -> None:
        with pytest.raises(ValueError, match="forbidden characters"):
            sanitize_extra_args("-ntmpi 2; rm -rf /", GMX_FORBIDDEN_FLAGS)

    def test_tuner_owned_gmx_flags_are_removed(self) -> None:
        assert sanitize_extra_args("-ntomp 4 -ntmpi 2", GMX_FORBIDDEN_FLAGS) == "-ntmpi 2"

    def test_tuner_owned_gmx_input_flag_is_removed(self) -> None:
        assert sanitize_extra_args("-s other.tpr -ntmpi 2", GMX_FORBIDDEN_FLAGS) == "-ntmpi 2"

    def test_tuner_owned_gmx_cpt_flag_is_removed(self) -> None:
        assert sanitize_extra_args("-cpt 30 -ntmpi 2", GMX_FORBIDDEN_FLAGS) == "-ntmpi 2"

    def test_tuner_owned_gmx_verbose_negation_is_removed(self) -> None:
        assert sanitize_extra_args("-nov -ntmpi 2", GMX_FORBIDDEN_FLAGS) == "-ntmpi 2"


class TestSanitizeAmberExtraArgs:
    def test_empty_string_returns_empty(self) -> None:
        assert sanitize_extra_args("", AMBER_FORBIDDEN_FLAGS) == ""

    def test_valid_amber_args_pass_through(self) -> None:
        result = sanitize_extra_args("-AllowSmallBox", AMBER_FORBIDDEN_FLAGS)
        assert result == "-AllowSmallBox"

    def test_forbidden_characters_raise(self) -> None:
        with pytest.raises(ValueError, match="forbidden characters"):
            sanitize_extra_args("-AllowSmallBox; rm -rf /", AMBER_FORBIDDEN_FLAGS)

    def test_tuner_owned_amber_input_flag_is_removed(self) -> None:
        assert sanitize_extra_args("-i custom.mdin -AllowSmallBox", AMBER_FORBIDDEN_FLAGS) == "-AllowSmallBox"

    def test_tuner_owned_amber_topology_flag_is_removed(self) -> None:
        assert sanitize_extra_args("-p other.prmtop -AllowSmallBox", AMBER_FORBIDDEN_FLAGS) == "-AllowSmallBox"

    def test_tuner_owned_amber_overwrite_flag_is_removed(self) -> None:
        assert sanitize_extra_args("-O -AllowSmallBox", AMBER_FORBIDDEN_FLAGS) == "-AllowSmallBox"

    def test_multiple_valid_args_pass_through(self) -> None:
        result = sanitize_extra_args("-AllowSmallBox -verbose", AMBER_FORBIDDEN_FLAGS)
        assert result == "-AllowSmallBox -verbose"

    def test_equals_syntax_tuner_owned_flag_is_removed(self) -> None:
        assert sanitize_extra_args("-i=custom.mdin -AllowSmallBox", AMBER_FORBIDDEN_FLAGS) == "-AllowSmallBox"
