(ns views.about
  "About view - system documentation and architecture."
  (:require [reagent.core :as r]))

(defn architecture-diagram []
  [:div.architecture-section
   [:h3 "🏗️ System Architecture"]
   [:div.diagram
    [:pre {:style {:background "#0f172a"
                   :padding "1.5rem"
                   :border-radius "8px"
                   :overflow-x "auto"
                   :color "#cbd5e1"
                   :font-family "monospace"
                   :font-size "0.85rem"
                   :line-height "1.6"}}
     "┌─────────────────────────────────────────────────────────────┐
│                    KAFKA MONITOR SYSTEM                     │
│              Event-Driven Microservices Demo                │
└─────────────────────────────────────────────────────────────┘

┌──────────────────┐
│ Order Processor  │  Generates 10 orders/second
│   (Producer)     │  • Random customer (1-100)
└────────┬─────────┘  • Random product (PROD-001 to 005)
         │            • Random quantity & price
         ↓
    ┌────────┐
    │ Kafka  │  Event Streaming Platform
    │ Topics │  • orders topic
    └────┬───┘  • registry topic
         │
    ┌────┴────────────────────────┐
    ↓                             ↓
┌─────────────────┐      ┌──────────────────┐
│ Query Processor │      │Registry Processor│
│   (Consumer)    │      │    (Consumer)    │
└────────┬────────┘      └────────┬─────────┘
         │                        │
         ↓                        ↓
    Aggregates:              Validates:
    • By Customer            • Price consistency
    • By Product             • Business rules
    • Timeline               → Approves/Denies
    • Total Revenue
         │                        │
         ↓                        ↓
    ┌────────────────────────────────┐
    │      Cassandra Database        │
    │   (Distributed NoSQL Store)    │
    └────────┬───────────────────────┘
             │
             ↓
        ┌────────┐
        │Monitor │  Real-time Dashboard
        │  (UI)  │  • ClojureScript + Re-frame
        └────────┘  • Ring + Compojure API"]]])

(defn tech-stack []
  [:div.tech-stack-section
   [:h3 "🛠️ Technology Stack"]
   [:div.tech-grid
    [:div.tech-card
     [:h4 "Backend"]
     [:ul
      [:li [:strong "Language:"] " Clojure 1.11"]
      [:li [:strong "Message Broker:"] " Apache Kafka 7.5"]
      [:li [:strong "Database:"] " Cassandra 4.1"]
      [:li [:strong "Cache:"] " Redis 7"]
      [:li [:strong "Web Server:"] " Ring + Compojure + Jetty"]]]

    [:div.tech-card
     [:h4 "Frontend"]
     [:ul
      [:li [:strong "Language:"] " ClojureScript"]
      [:li [:strong "Framework:"] " Reagent + Re-frame"]
      [:li [:strong "State:"] " Re-frame (Reactive)"]
      [:li [:strong "HTTP:"] " cljs-ajax"]
      [:li [:strong "Build:"] " Leiningen + cljsbuild"]]]

    [:div.tech-card
     [:h4 "Infrastructure"]
     [:ul
      [:li [:strong "Containers:"] " Docker + Docker Compose"]
      [:li [:strong "Services:"] " 8 containers"]
      [:li [:strong "Network:"] " Bridge network"]
      [:li [:strong "Persistence:"] " Named volumes"]
      [:li [:strong "Health checks:"] " All services"]]]

    [:div.tech-card
     [:h4 "Patterns"]
     [:ul
      [:li [:strong "Architecture:"] " Event-Driven"]
      [:li [:strong "Data:"] " CQRS (Command Query)"]
      [:li [:strong "Views:"] " Materialized Views"]
      [:li [:strong "Processing:"] " Stream Processing"]
      [:li [:strong "State:"] " Event Sourcing"]]]]])

(defn features []
  [:div.features-section
   [:h3 "✨ Key Features"]
   [:div.features-grid
    [:div.feature-card
     [:div.feature-icon "⚡"]
     [:h4 "Real-time Processing"]
     [:p "Orders processed at 10/second with sub-second latency"]]

    [:div.feature-card
     [:div.feature-icon "📊"]
     [:h4 "Materialized Views"]
     [:p "Pre-computed aggregations for fast queries"]]

    [:div.feature-card
     [:div.feature-icon "🔄"]
     [:h4 "Event Streaming"]
     [:p "Kafka handles message distribution and replay"]]

    [:div.feature-card
     [:div.feature-icon "📈"]
     [:h4 "Scalable Design"]
     [:p "Microservices can scale independently"]]

    [:div.feature-card
     [:div.feature-icon "🎯"]
     [:h4 "CQRS Pattern"]
     [:p "Separate read and write models for optimization"]]

    [:div.feature-card
     [:div.feature-icon "🔍"]
     [:h4 "Real-time Monitoring"]
     [:p "Dashboard updates automatically with live data"]]]])

(defn limitations []
  [:div.limitations-section
   [:h3 "⚠️ Known Limitations"]
   [:div.info-box.warning
    [:p [:strong "This is an educational demo, not production-ready."]]
    [:ul
     [:li "Orders are auto-generated - no manual creation via UI"]
     [:li "No authentication or authorization"]
     [:li "No order state transitions (pending → approved/denied)"]
     [:li "Limited error handling and retry logic"]
     [:li "No distributed tracing or comprehensive logging"]
     [:li "Registry processor validates but doesn't persist rejections"]
     [:li "No rate limiting or backpressure handling"]]]

   [:div.info-box.info
    [:h4 "💡 Future Improvements"]
    [:ul
     [:li "Add POST /api/orders endpoint for manual order creation"]
     [:li "Implement WebSocket for real-time updates"]
     [:li "Add order search by ID with full history"]
     [:li "Implement proper state machine for order lifecycle"]
     [:li "Add metrics and monitoring (Prometheus + Grafana)"]
     [:li "Implement saga pattern for distributed transactions"]]]])

(defn getting-started []
  [:div.getting-started-section
   [:h3 "🚀 Getting Started"]
   [:div.code-block
    [:h4 "Prerequisites"]
    [:pre "• Docker & Docker Compose
- 8GB RAM minimum
- Ports: 3000, 8080, 9042, 9092, 2181, 6379"]

    [:h4 "Quick Start"]
    [:pre "# Clone repository
git clone <repo-url>
cd kafka-monitor

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Access dashboard
http://localhost:3000

# Stop services
docker-compose down"]

    [:h4 "Development"]
    [:pre "# Rebuild specific service
docker-compose build monitor

# Restart service
docker-compose restart monitor

# View Cassandra data
docker exec -it kafka-monitor-cassandra cqlsh

# View Kafka topics
docker exec -it kafka-monitor-kafka \\
  kafka-topics --list \\
  --bootstrap-server localhost:9092"]]])

(defn about-view []
  [:div.about-view
   [:div.view-header
    [:h1 "📚 About Kafka Monitor"]
    [:p.subtitle "Educational demonstration of event-driven microservices architecture"]]

   [:div.about-content
    [architecture-diagram]
    [tech-stack]
    [features]
    [limitations]
    [getting-started]

    [:div.footer-section
     [:p.footer-text "Built with ❤️ using Clojure & ClojureScript"]
     [:p.footer-text "For educational and test purposes • Not production-ready"]]]])