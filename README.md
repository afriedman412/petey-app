<img src="static/logo-dark.png">

**Petey: A framework for PDF data extraction.**

Download for [Mac and Windows](https://github.com/afriedman412/petey-app/releases/latest) | Run with [Docker](https://hub.docker.com/r/afriedman412/petey) | Try the [live demo](https://petey.cc/demos)

---

The PDF was invented in 1991 by Adobe co-founder John Warnock. While the future was digital, the present was analog, and the new format was intended as a bridge between the two eras. PDFs were designed to be a universal container for the printed page that would look the same on any screen or printer. A PDF generated in California should be identical to a version printed in London.

Warnock achieved his goal, and today PDFs are ubiquitous. [According to *The Economist*](https://www.economist.com/business/2026/02/24/the-war-against-pdfs-is-heating-up), there are over 2.4 trillion PDF-formatted documents across the world's computers. They are your bank statement after you go paperless. They are the only existing copy of a newspaper article. They are the textbook for an online class. They are the forms you fill out when you join a gym. They are everywhere.

The secret to their flexibility is that they have almost no rules internally. A PDF is just a list of items — words, characters, shapes, images — and their coordinates on the page. No information about the relationship between anything. Two words that appear next to each other in print could be 1,000 lines apart in code. (If you have ever tried to highlight a line in a PDF and ended up selecting words halfway across the page, that's why.)

This makes PDFs a nightmare to work with.

But people have been working on PDF extraction for a long time. Open-source tools like [pdfplumber](https://github.com/jsvine/pdfplumber) and [PyMuPDF](https://github.com/pymupdf/PyMuPDF) can do almost everything, and the commercial options are even better. AI can finish the job, and it's cheap and fast enough for everyone to use. The only thing standing between you and the data in your PDF is something to bring it all together.

## What is Petey?

Petey is a framework for PDF data extraction. It wires the PDF parser of your choice to the LLM of your choice, and, with input from the user, pulls the data out of your PDF document.

## What <i>isn't</i> Petey?

Petey is not a parser or an LLM itself. It outsources most of its tasks to other services, whether they are open-source or commercial. You bring your own API keys to Petey, and you pay for everything Petey does to your documents. Typical cost is 1-5 cents per page.

## Get started

### Web (no install)

Try the [interactive demos](https://petey.cc/demos) or go straight to the [extractor](https://petey.cc).

### Desktop app

Download the latest release for [Mac or Windows](https://github.com/afriedman412/petey-app/releases/latest). Open the app, add [an API key](https://petey.cc/about#keys) in Settings, and you're ready to go.

### Docker

```sh
docker run -p 8080:8080 afriedman412/petey
```

Open [http://localhost:8080](http://localhost:8080).

### Python CLI

```sh
pip install petey
petey extract --schema your_schema.yaml document.pdf
petey extract --schema your_schema.yaml folder/ -o results.csv
```

See `petey extract --help` for all options.

## API keys

Petey connects to external services for parsing and extraction. You'll need at least one API key to run extractions. See the [API key setup guide](https://petey.cc/about#keys) for step-by-step instructions.

| Provider | What it does | Cost |
|----------|-------------|------|
| [OpenAI](https://platform.openai.com) | LLM extraction | ~$0.01-0.05/page |
| [Anthropic](https://console.anthropic.com) | LLM extraction | ~$0.01-0.04/page |
| [Datalab (Marker)](https://www.datalab.to) | AI-powered parsing | ~$0.005/page |
| [PyMuPDF](https://github.com/pymupdf/PyMuPDF) | Built-in parsing | Free |

## Links

- [Live app](https://petey.cc)
- [Demos](https://petey.cc/demos)
- [API key setup](https://petey.cc/about#keys)
- [Costs](https://petey.cc/about#costs)
- [FAQ](https://petey.cc/about#faq)
- [Python package (GitHub)](https://github.com/afriedman412/petey)
- [PyPI](https://pypi.org/project/petey/)
- [Docker Hub](https://hub.docker.com/r/afriedman412/petey)
