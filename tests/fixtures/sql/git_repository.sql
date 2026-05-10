CREATE OR REPLACE GIT REPOSITORY my_git_repository
   ORIGIN = 'https://github.com/some-org/some-repo.git'
   API_INTEGRATION = my_api_integration
   GIT_CREDENTIALS = my_git_secret
   COMMENT = 'Example git repository';
