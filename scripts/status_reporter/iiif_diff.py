from deepdiff import DeepDiff

class IIIFDiff:
    def __init__(self, library_iif):
        self.library_iif = library_iif

    def _update_value(self, obj, is_key, convert):
        if isinstance(obj, (int, float)):
            return obj
        if isinstance(obj, (str)):
            return convert(obj)
        if isinstance(obj, dict):
            new = obj.__class__()

            for k, v in obj.items():
                if(is_key(k)):
                    new[k] = convert(v)
                else:
                    new[k] = self._update_value(v, is_key, convert)

        elif isinstance(obj, (list, set, tuple)):
            new = obj.__class__(self._update_value(v, is_key, convert) for v in obj)
        else:
            return obj
        return new

    def _match(self, value):
        is_id = value == '@id'
        is_on = value == 'on'

        return is_id or is_on

    def _convert(self, value):
        return value.replace('library-uat.', '')

    def _diff_summary(self, difference):
        diff_types = [k for k,v in difference.items()]

        summary = {
            'images': [],
            'values': [],
            'types': []

        }

        if(len(diff_types) == 0):
            return summary

        type_changes = 'type_changes' in diff_types
        values_changed = 'values_changed' in diff_types

        expected_diff_types = type_changes or values_changed

        assert expected_diff_types, f"Found insertions/deletions! {diff_types}"

        if(values_changed):
            for key,diff in difference['values_changed'].items():
                if(key.endswith("['@id']")):
                    summary_key = 'images'
                else:
                    summary_key = 'values'

                summary[summary_key].append({
                    'key': key,
                    'old_value': diff['old_value'],
                    'new_value': diff['new_value'],
                })

        if(type_changes):
            for key,diff in difference['type_changes'].items():
                summary['types'].append({
                    'key': key,
                    'old_value': diff['old_value'],
                    'new_value': diff['new_value'],
                })

        return summary

    def _check_response(self, response, name):
        status_ok = response['status'] == 'ok'

        if(not status_ok):
            raise Exception(f"Got status {response['status']} for {name}")

    def diff(self, bnum):
        bnum_stage = self.library_iif.stage(bnum)
        bnum_prod = self.library_iif.prod(bnum)

        self._check_response(bnum_stage, 'stage')
        self._check_response(bnum_prod, 'prod')

        bnum_stage_converted = self._update_value(bnum_stage['body'], self._match, self._convert)
        bnum_prod_converted = self._update_value(bnum_prod['body'], self._match, self._convert)

        deep_diff = DeepDiff(bnum_stage_converted, bnum_prod_converted)

        return self._diff_summary(deep_diff)