# -*- encoding: utf-8

import abc

import dynamo_status_manager
import helpers
import reporting


class MigrationCheck(abc.ABC):
    # Base class for migration checks.
    # Must define:
    #
    #   self.previous_check
    #   self.current_check
    #   self.check_name

    def needs_check(self, status_summary):
        return helpers.needs_check(
            status_summary,
            previous_check=self.previous_check,
            current_check=self.current_check,
            step_name=self.check_name
        )

    def get_statuses_for_updating(self, first_bnumber):
        reader = dynamo_status_manager.DynamoStatusReader()

        for status_summary in reader.all(first_bnumber=first_bnumber):
            if self.needs_check(status_summary):
                yield status_summary

    @abc.abstractmethod
    def run_check(self, status_updater, status_summary):
        pass

    def run_all(self, first_bnumber=None):
        all_statuses = self.get_statuses_for_updating(first_bnumber=first_bnumber)

        with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
            for status_summary in all_statuses:
                self.run_check(
                    status_updater=status_updater,
                    status_summary=status_summary
                )

    def run_one(self, bnumber):
        reader = dynamo_status_manager.DynamoStatusReader()
        status_summary = reader.get_one(bnumber)

        if self.needs_check(status_summary):
            with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
                self.run_check(
                    status_updater=status_updater,
                    status_summary=status_summary
                )

    def report(self):
        return reporting.build_report(name=self.current_check)
