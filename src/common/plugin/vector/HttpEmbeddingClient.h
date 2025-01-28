class HttpEmbeddingClient : public EmbeddingClient {
public:
    explicit HttpEmbeddingClient(std::shared_ptr<HttpClient> client,
                                std::string url = FLAGS_vector_embedding_url,
                                std::string authToken = FLAGS_vector_embedding_auth_token)
        : client_(client)
        , url_(std::move(url))
        , authToken_(std::move(authToken)) {}
    // ...
private:
    std::shared_ptr<HttpClient> client_;
    std::string url_;
    std::string authToken_;
}; 