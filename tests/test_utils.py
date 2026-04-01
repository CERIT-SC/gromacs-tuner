import pytest

from api.utils import sanitize_amber_extra_args, sanitize_extra_args


class TestSanitizeExtraArgs:
    def test_empty_string_returns_empty(self):
        assert sanitize_extra_args("") == ""

    def test_valid_args_pass_through(self):
        result = sanitize_extra_args("-ntmpi 2")
        assert result == "-ntmpi 2"

    def test_forbidden_characters_raise(self):
        with pytest.raises(ValueError, match="forbidden characters"):
            sanitize_extra_args("-ntmpi 2; rm -rf /")

    def test_forbidden_gmx_flags_raise(self):
        with pytest.raises(ValueError, match="critical"):
            sanitize_extra_args("-ntomp 4")

    def test_forbidden_flag_s_raises(self):
        with pytest.raises(ValueError, match="critical"):
            sanitize_extra_args("-s other.tpr")


class TestSanitizeAmberExtraArgs:
    def test_empty_string_returns_empty(self):
        assert sanitize_amber_extra_args("") == ""

    def test_valid_amber_args_pass_through(self):
        result = sanitize_amber_extra_args("-AllowSmallBox")
        assert result == "-AllowSmallBox"

    def test_forbidden_characters_raise(self):
        with pytest.raises(ValueError, match="forbidden characters"):
            sanitize_amber_extra_args("-AllowSmallBox; rm -rf /")

    def test_forbidden_amber_flag_i_raises(self):
        with pytest.raises(ValueError, match="critical"):
            sanitize_amber_extra_args("-i custom.mdin")

    def test_forbidden_amber_flag_p_raises(self):
        with pytest.raises(ValueError, match="critical"):
            sanitize_amber_extra_args("-p other.prmtop")

    def test_forbidden_amber_flag_O_raises(self):
        with pytest.raises(ValueError, match="critical"):
            sanitize_amber_extra_args("-O")
