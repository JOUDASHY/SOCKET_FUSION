// server.js
// =====================================================
// Importations
// =====================================================
const express = require('express');
const http = require('http');
const cors = require('cors');
const WebSocket = require('ws');
const { Server: SocketIOServer } = require('socket.io');

// =====================================================
// Création de l'application Express et du serveur HTTP
// =====================================================
const app = express();
const server = http.createServer(app);

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());

// =====================================================
// 1. Serveur de Messagerie (Messages)
// Chemin WS : /ws/messages
// =====================================================

// Stockage des clients connectés pour les messages
const messagesClients = {};

// Création du WebSocket server pour les messages (mode noServer)
const wssMessages = new WebSocket.Server({ noServer: true });

wssMessages.on('connection', (ws, req) => {
  console.log('Messages WS: Client connecté');
  ws.on('message', (data) => {
    try {
      const parsedData = JSON.parse(data);
      if (parsedData.userId) {
        messagesClients[parsedData.userId] = ws;
      }
    } catch (err) {
      console.error('Messages WS: Erreur lors du parsing du message:', err);
    }
  });
  ws.on('close', () => {
    console.log('Messages WS: Client déconnecté');
    Object.keys(messagesClients).forEach((userId) => {
      if (messagesClients[userId] === ws) {
        delete messagesClients[userId];
      }
    });
  });
});

// Endpoint HTTP pour diffuser un message (POST depuis Laravel, par exemple)
app.post('/api/messages/broadcast', (req, res) => {
  const { sender_id, receiver_id, message, attachment, created_at } = req.body;
  const fullMessage = JSON.stringify({
    sender_id,
    receiver_id,
    message,
    created_at,
    attachment,
  });
  console.log('Messages WS: Diffusion du message:', fullMessage);
  if (messagesClients[receiver_id]) {
    messagesClients[receiver_id].send(fullMessage);
    console.log(`Messages WS: Message envoyé à l'utilisateur ${receiver_id}`);
  } else {
    console.log(`Messages WS: L'utilisateur ${receiver_id} n'est pas connecté`);
  }
  res.status(200).send('Message diffusé');
});

// =====================================================
// 2. Serveur de Notifications
// Chemin WS : /ws/notifications
// =====================================================

// Stockage des clients pour les notifications
const notificationsClients = {};

// Création du WebSocket server pour les notifications (mode noServer)
const wssNotifications = new WebSocket.Server({ noServer: true });

wssNotifications.on('connection', (ws, req) => {
  // On utilise le header 'sec-websocket-protocol' pour transmettre l'ID utilisateur
  const userId = req.headers['sec-websocket-protocol'];
  if (!userId) {
    ws.close();
    console.error('Notifications WS: Pas d\'ID utilisateur fourni, connexion fermée.');
    return;
  }
  notificationsClients[userId] = ws;
  console.log(`Notifications WS: Utilisateur connecté: ${userId}`);
  ws.on('close', () => {
    delete notificationsClients[userId];
    console.log(`Notifications WS: Utilisateur déconnecté: ${userId}`);
  });
  ws.on('error', (error) => {
    console.error(`Notifications WS: Erreur sur le WS de l'utilisateur ${userId}:`, error);
  });
});

// Endpoint HTTP pour diffuser une notification
app.post('/api/notifications/broadcast', (req, res) => {
  const { userId, message } = req.body;
  if (notificationsClients[userId]) {
    notificationsClients[userId].send(JSON.stringify({ message }));
    console.log(`Notifications WS: Notification envoyée à l'utilisateur ${userId}`);
    res.status(200).send({ status: 'Message diffusé' });
  } else {
    console.warn(`Notifications WS: L'utilisateur ${userId} n'est pas connecté, message non envoyé.`);
    res.status(404).send({ status: 'Utilisateur non connecté' });
  }
});

// =====================================================
// 3. Serveur des Utilisateurs en Ligne (Online Users)
// Chemin WS : /ws/onlineUsers
// =====================================================

// Utilisation d'une Map pour suivre les utilisateurs en ligne
const onlineUsers = new Map();

// Création du WebSocket server pour les utilisateurs en ligne (mode noServer)
const wssOnlineUsers = new WebSocket.Server({ noServer: true });

wssOnlineUsers.on('connection', (ws, req) => {
  console.log('OnlineUsers WS: Client connecté');
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      const { userId } = data;
      if (userId) {
        onlineUsers.set(userId, ws);
        broadcastOnlineUsers();
        console.log(`OnlineUsers WS: ${userId} connecté`);
      }
    } catch (err) {
      console.error('OnlineUsers WS: Erreur lors du parsing du message:', err);
    }
  });
  ws.on('close', () => {
    const userId = [...onlineUsers.entries()].find(([, client]) => client === ws)?.[0];
    if (userId) {
      onlineUsers.delete(userId);
      broadcastOnlineUsers();
    }
    console.log('OnlineUsers WS: Client déconnecté');
  });
});

// Fonction pour diffuser la liste des utilisateurs en ligne à tous les clients connectés
function broadcastOnlineUsers() {
  const userIds = Array.from(onlineUsers.keys());
  const message = JSON.stringify({ type: 'onlineUsers', users: userIds });
  onlineUsers.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// =====================================================
// 4. Serveur de Conférence Vidéo (Video Conference)
// Utilisation de Socket.IO sous le namespace "/video"
// =====================================================
const io = new SocketIOServer(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
});

// Définition du namespace "/video"
const videoNamespace = io.of('/video');

let activeConference = false;
let rooms = {};

videoNamespace.on('connection', (socket) => {
  console.log('Video (/video): Utilisateur connecté:', socket.id);
  socket.on('checkConferenceStatus', (callback) => {
    console.log('Video (/video): Vérification de l\'état de la conférence...');
    callback(activeConference);
  });
  socket.on('checkRoomAvailability', (roomName, callback) => {
    if (rooms[roomName]) {
      callback(false);
    } else {
      callback(true);
    }
  });
  socket.on('broadcaster', (roomName) => {
    if (!roomName) {
      console.log('Video (/video): Nom de salle non défini ou invalide pour le broadcaster');
      return;
    }
    if (!rooms[roomName]) {
      rooms[roomName] = { broadcaster: socket.id, watchers: [] };
      socket.join(roomName);
      videoNamespace.to(roomName).emit('broadcaster', roomName);
      console.log(`Video (/video): Broadcaster démarré dans la salle: ${roomName}`);
      if (!activeConference) {
        activeConference = true;
        console.log('Video (/video): Une conférence a démarré.');
      }
    } else {
      console.log(`Video (/video): La salle ${roomName} est déjà occupée`);
    }
  });
  socket.on('watcher', (roomName) => {
    if (!roomName) {
      console.log('Video (/video): Nom de salle non défini ou invalide pour le watcher');
      return;
    }
    const room = rooms[roomName];
    if (room && room.broadcaster) {
      room.watchers.push(socket.id);
      socket.join(roomName);
      socket.to(room.broadcaster).emit('watcher', socket.id);
      console.log(`Video (/video): Watcher ${socket.id} a rejoint la salle: ${roomName}`);
    } else {
      console.log(`Video (/video): La salle ${roomName} n'est pas disponible pour les watchers`);
    }
  });
  socket.on('offer', (id, message) => {
    socket.to(id).emit('offer', socket.id, message);
  });
  socket.on('answer', (id, message) => {
    socket.to(id).emit('answer', socket.id, message);
  });
  socket.on('candidate', (id, message) => {
    socket.to(id).emit('candidate', socket.id, message);
  });
  socket.on('endConference', (roomName) => {
    if (rooms[roomName]) {
      delete rooms[roomName];
      videoNamespace.to(roomName).emit('endConference');
      console.log(`Video (/video): La salle ${roomName} est désormais libre`);
      activeConference = false;
      console.log('Video (/video): Conférence terminée.');
    } else {
      console.log(`Video (/video): La salle ${roomName} n'existe pas`);
    }
  });
  socket.on('disconnect', () => {
    Object.keys(rooms).forEach((roomName) => {
      const room = rooms[roomName];
      if (room.watchers.includes(socket.id)) {
        room.watchers = room.watchers.filter(id => id !== socket.id);
        socket.to(room.broadcaster).emit('disconnectPeer', socket.id);
      }
      if (room.broadcaster === socket.id) {
        delete rooms[roomName];
        videoNamespace.to(roomName).emit('endConference');
        console.log(`Video (/video): La salle ${roomName} est fermée (broadcaster déconnecté)`);
        activeConference = false;
        console.log('Video (/video): Conférence terminée à cause de la déconnexion du broadcaster.');
      }
    });
  });
});

// =====================================================
// Gestion manuelle des requêtes d'upgrade (HTTP → WebSocket)
// =====================================================
server.on('upgrade', (request, socket, head) => {
  const { url } = request;
  if (url.startsWith('/ws/messages')) {
    wssMessages.handleUpgrade(request, socket, head, (ws) => {
      wssMessages.emit('connection', ws, request);
    });
  } else if (url.startsWith('/ws/notifications')) {
    wssNotifications.handleUpgrade(request, socket, head, (ws) => {
      wssNotifications.emit('connection', ws, request);
    });
  } else if (url.startsWith('/ws/onlineUsers')) {
    wssOnlineUsers.handleUpgrade(request, socket, head, (ws) => {
      wssOnlineUsers.emit('connection', ws, request);
    });
  } else {
    // Pour les autres chemins (ex : Socket.IO qui utilise /socket.io)
    // Laissez Socket.IO (ou tout autre middleware) gérer l'upgrade.
  }
});

// =====================================================
// Lancement du serveur
// =====================================================
const PORT = 8110;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Serveur démarré sur le port ${PORT}`);
  console.log('Endpoints disponibles :');
  console.log('  - WS Messages       : ws://<hostname>:8110/ws/messages');
  console.log('  - WS Notifications  : ws://<hostname>:8110/ws/notifications');
  console.log('  - WS Online Users   : ws://<hostname>:8110/ws/onlineUsers');
  console.log('  - Socket.IO Video   : http://<hostname>:8110/socket.io/ (namespace "/video")');
});
