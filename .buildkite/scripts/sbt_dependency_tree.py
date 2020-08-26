#!/usr/bin/env python
# -*- encoding: utf-8

import json
import os


class Project:
    def __init__(self, name, folder, dependencies):
        self.name = name
        self.folder = folder
        self.dependencies = dependencies

    def __repr__(self):
        return "<%s name=%r folder=%r dependencies=%r>" % (
            type(self).__name__,
            self.name,
            self.folder,
            ",".join(sorted(d.name for d in self.dependencies)),
        )

    def all_folders(self):
        folders = set([self.folder])
        for d in self.dependencies:
            folders |= d.all_folders()
        return folders

    def depends_on(self, other):
        if self == other:
            return True
        if other in self.dependencies:
            return True
        if any(d.depends_on(other) for d in self.dependencies):
            return True

        return False


class Repository:
    def __init__(self, metadata_dir):
        self.metadata_dir = metadata_dir
        self.projects = {}

        self._get_all_projects()

    def _get_all_projects(self):
        for f in os.listdir(self.metadata_dir):
            self.get_project(name=os.path.splitext(f)[0])

    def get_project(self, name):
        if name not in self.projects:
            data = json.load(open(os.path.join(self.metadata_dir, name + ".json")))

            project = Project(
                name=data["id"],
                folder=data["folder"],
                dependencies=[self.get_project(d) for d in data["dependencyIds"]],
            )

            self.projects[name] = project

        return self.projects[name]

    def lookup_path(self, path):
        """Which project, if any, is this path associated with?"""
        for project in self.projects.values():
            """We check with a / at the end to make sure we don't match partial folder names"""
            if path.startswith(f"{project.folder}/"):
                return project
        else:
            raise KeyError("No project associated with %r!" % path)
