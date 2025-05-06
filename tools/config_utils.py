# -*- encoding: utf-8 -*-
import os
import yaml
import argparse


class ConfigUtils:

    @staticmethod
    def parse_config(args):
        config_file_name = args.config_file
        args.config_file = config_file_name
        args = ConfigUtils.parse_file_config(args)
        return args

    @staticmethod
    def parse_file_config(args, config_file_root=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))):
        config_file_name = args.config_file
        config_file_path = os.path.join(config_file_root, 'configs', config_file_name)

        if not os.path.exists(config_file_path):
            print("[error] wrong config file: {}".format(config_file_name))
            raise Exception("wrong config file: {}".format(config_file_name))

        with open(config_file_path, encoding='utf-8') as f:
            content = f.read()
            config_dict = yaml.safe_load(content)
            ConfigUtils.convert_dict_to_args(args, config_dict)
        return args

    @staticmethod
    def convert_dict_to_args(args, config_dict):
        for key in config_dict:
            value = config_dict[key]

            if type(value) == dict:
                if not hasattr(args, key):
                    setattr(args, key, argparse.Namespace())
                ConfigUtils.convert_dict_to_args(getattr(args, key), value)

            else:
                setattr(args, key, value)