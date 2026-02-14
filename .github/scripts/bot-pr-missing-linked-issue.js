module.exports = async ({ github, context }) => {
  const body = context.payload.pull_request.body || "";
  const regex = /\bFixes\s*:?\s*(#\d+)(\s*,\s*#\d+)*/i;

  const comments = await github.rest.issues.listComments({
  owner: context.repo.owner,
  repo: context.repo.repo,
  issue_number: context.payload.pull_request.number,
  });

  const alreadyCommented = comments.data.some(comment =>
    comment.body.includes("this is LinkBot")
  );

  if (alreadyCommented) {
    return;
  }

  if (!regex.test(body)) {
    await github.rest.issues.createComment({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: context.payload.pull_request.number,
      body: [
        `Hi @${context.payload.pull_request.user.login}, this is **LinkBot** 👋`,
        ``,
        `Linking pull requests to issues helps us significantly with reviewing pull requests and keeping the repository healthy.`,
        ``,
        `🚨 **This pull request does not have an issue linked.**`,
        ``,
        `Please link an issue using the following format:`,
        `- Fixes #123`,
        ``,
        `📖 Guide:`,
        `[Documentation/SETUP.md](https://github.com/${context.repo.owner}/${context.repo.repo}/blob/main/Documentation/SETUP.md)`,
        ``,
        `If no issue exists yet, please create one:`,
        `[Documentation/user.md](https://github.com/${context.repo.owner}/${context.repo.repo}/blob/main/Documentation/user.md)`,
        ``,
        `Thanks!`
      ].join('\n')
    });
  }
};

