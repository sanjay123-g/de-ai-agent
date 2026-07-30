"""
streamlit_app.py — minimal dashboard calling the FastAPI query endpoint.
Run: streamlit run streamlit_app.py
(Requires the FastAPI server running: uvicorn api:app --reload)
"""
import streamlit as st
import requests

st.set_page_config(page_title="de-ai-agent", page_icon="soccer")
st.title("de-ai-agent — Ask a question about World Cup 2026 data")
st.caption("Natural language to SQL to Snowflake, via a local qwen2.5-coder:14b model")

API_URL = "http://localhost:8000/v1/nl-to-sql"

if "history" not in st.session_state:
    st.session_state.history = []

for h in st.session_state.history:
    st.chat_message("user").write(h["question"])
    st.chat_message("assistant").write(h.get("summary", ""))

question = st.text_input(
    "Ask a question",
    placeholder="e.g. Which team has the highest win percentage?",
)

if st.button("Ask") and question:
    with st.spinner("Thinking..."):
        try:
            resp = requests.post(
                API_URL,
                json={"question": question, "history": st.session_state.history},
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("summary"):
                st.success(data["summary"])
                st.session_state.history.append({"question": question, "summary": data["summary"]})

            if data.get("sql"):
                st.subheader("Generated SQL")
                st.code(data["sql"], language="sql")

            if data.get("rows"):
                st.subheader("Results")
                st.dataframe(
                    {col: [row[i] for row in data["rows"]] for i, col in enumerate(data["columns"])}
                )
                try:
                    labels = [str(r[0]) for r in data["rows"]]
                    values = [r[-1] for r in data["rows"]]
                    if all(isinstance(v, (int, float)) for v in values):
                        st.bar_chart({data["columns"][0]: labels, data["columns"][-1]: values})
                except Exception:
                    pass

        except requests.exceptions.ConnectionError:
            st.error("Cannot reach the API. Run `uvicorn api:app --reload` first.")
        except requests.exceptions.HTTPError as e:
            st.warning(resp.json().get("detail", str(e)))
        except Exception as e:
            st.error(f"Error: {e}")
